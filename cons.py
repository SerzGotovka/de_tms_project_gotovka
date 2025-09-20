import json
import time
from typing import Any, Dict, List, Optional, Type, Union, Callable
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from models.pydantic.pydantic_models import (
    Photo)
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


def process_message_batch(
    batch_messages: List[tuple], 
    insert_function: Optional[Callable], 
    insert_params: Optional[Dict[str, Any]],
    processed_ids: set,
    lock: threading.Lock
) -> int:
    """
    Обрабатывает батч сообщений параллельно
    
    Args:
        batch_messages: Список кортежей (validated_data, data_id)
        insert_function: Функция для вставки данных
        insert_params: Параметры для функции вставки
        processed_ids: Множество обработанных ID (thread-safe)
        lock: Блокировка для thread-safe операций
    
    Returns:
        int: Количество успешно обработанных сообщений
    """
    success_count = 0
    
    if not insert_function:
        # Если нет функции вставки, просто считаем сообщения обработанными
        with lock:
            for validated_data, data_id in batch_messages:
                if data_id is not None:
                    processed_ids.add(data_id)
        return len(batch_messages)
    
    # Обрабатываем каждое сообщение в батче
    for validated_data, data_id in batch_messages:
        try:
            if insert_params:
                success = insert_function(validated_data, **insert_params)
            else:
                success = insert_function(validated_data)
            
            if success and data_id is not None:
                with lock:
                    processed_ids.add(data_id)
                success_count += 1
            elif not success:
                print(f"Не удалось вставить данные с ID {data_id}")
        except Exception as e:
            print(f"Ошибка при вставке данных с ID {data_id}: {e}")
    
    return success_count


def batch_insert_into_postgres(
    data_list: List[Any],
    table_name: str,
    schema: str = "dds",
    postgres_conn_id: str = "my_postgres_conn",
    insert_columns: List[str] = None
) -> int:
    """
    Оптимизированная функция для батчевой вставки данных в PostgreSQL
    
    Args:
        data_list: Список объектов данных для вставки
        table_name: Имя таблицы для вставки
        schema: Схема базы данных (по умолчанию 'dds')
        postgres_conn_id: ID подключения к PostgreSQL в Airflow
        insert_columns: Список колонок для вставки
    
    Returns:
        int: Количество успешно вставленных записей
    """
    if not data_list:
        return 0
        
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Подготавливаем данные для батчевой вставки
        if insert_columns:
            columns = insert_columns
            values_list = []
            for data in data_list:
                values = []
                for col in columns:
                    if hasattr(data, col):
                        values.append(getattr(data, col))
                    else:
                        values.append(None)
                values_list.append(tuple(values))
        else:
            # Используем все атрибуты первого объекта как колонки
            first_data = data_list[0]
            columns = list(first_data.__dict__.keys())
            values_list = [tuple(getattr(data, col) for col in columns) for data in data_list]
        
        # Создаем SQL запрос для батчевой вставки
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        query = f"""
            INSERT INTO {schema}.{table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (id) DO NOTHING
        """
        
        # Выполняем батчевую вставку
        cursor.executemany(query, values_list)
        conn.commit()
        
        return len(data_list)
        
    except Exception as e:
        print(f"Ошибка при батчевой вставке в {table_name}: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()


def universal_insert_into_postgres(
    data: Any,
    table_name: str,
    schema: str = "dds",
    postgres_conn_id: str = "my_postgres_conn",
    check_dependency: Optional[Dict[str, str]] = None,
    insert_columns: List[str] = None,
    insert_values: List[Any] = None
) -> bool:
    """
    Универсальная функция для вставки данных в PostgreSQL с проверкой на дубликаты
    
    Args:
        data: Объект данных для вставки
        table_name: Имя таблицы для вставки
        schema: Схема базы данных (по умолчанию 'dds')
        postgres_conn_id: ID подключения к PostgreSQL в Airflow
        check_dependency: Словарь для проверки зависимостей {table: column}
        insert_columns: Список колонок для вставки (если не указан, используется data.__dict__)
        insert_values: Список значений для вставки (если не указан, используется data.__dict__.values())
    
    Returns:
        bool: True если вставка успешна, False в противном случае
    """
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Проверка зависимостей если указаны
        if check_dependency:
            for dep_table, dep_column in check_dependency.items():
                dep_value = getattr(data, dep_column, None)
                if dep_value is not None:
                    cursor.execute(f"SELECT id FROM {schema}.{dep_table} WHERE id = %s", (dep_value,))
                    if not cursor.fetchone():
                        print(f"Зависимость не найдена: {dep_table}.{dep_column} = {dep_value}. Пропускаем запись {getattr(data, 'id', 'unknown')}")
                        return False

        # Подготовка данных для вставки
        if insert_columns is None or insert_values is None:
            data_dict = data.__dict__ if hasattr(data, '__dict__') else data
            if isinstance(data_dict, dict):
                insert_columns = list(data_dict.keys())
                insert_values = list(data_dict.values())
            else:
                print("Ошибка: не удалось извлечь данные из объекта")
                return False

        # Проверка на существование записи с таким ID
        data_id = getattr(data, 'id', None)
        if data_id is not None:
            cursor.execute(f"SELECT id FROM {schema}.{table_name} WHERE id = %s", (data_id,))
            if cursor.fetchone():
                print(f"Запись с ID {data_id} уже существует в {schema}.{table_name}, пропускаем")
                return False

        # Формирование SQL запроса
        placeholders = ', '.join(['%s'] * len(insert_columns))
        columns_str = ', '.join(insert_columns)
        
        sql = f"""
            INSERT INTO {schema}.{table_name} ({columns_str})
            VALUES ({placeholders})
        """
        
        cursor.execute(sql, insert_values)
        conn.commit()
        print(f"Данные успешно добавлены в {schema}.{table_name}")
        return True
        
    except Exception as e:
        print(f"Ошибка при вставке данных в {schema}.{table_name}: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


