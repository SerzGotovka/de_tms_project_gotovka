import json
import time
from typing import Any, Dict, List, Optional, Type, Union
from confluent_kafka import Consumer, KafkaError
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from models.pydantic.pydantic_models import (
    Photo)


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


def universal_kafka_consumer(
    topics: Union[str, List[str]],
    group_id: str = "universal_consumer_group",
    bootstrap_servers: str = "kafka:9092",
    auto_offset_reset: str = "earliest",
    max_runtime: int = 300,
    max_messages: int = 100,
    poll_timeout: float = 1.0,
    data_model: Type[Any] = None,
    insert_function: callable = None,
    insert_params: Dict[str, Any] = None
) -> int:
    """
    Универсальная функция для работы с Kafka Consumer
    
    Args:
        topics: Топик(и) для подписки
        group_id: ID группы потребителя
        bootstrap_servers: Серверы Kafka
        auto_offset_reset: Стратегия сброса offset
        max_runtime: Максимальное время работы в секундах
        max_messages: Максимальное количество сообщений для обработки
        poll_timeout: Таймаут для poll в секундах
        data_model: Pydantic модель для валидации данных
        insert_function: Функция для вставки данных
        insert_params: Параметры для функции вставки
    
    Returns:
        int: Количество обработанных сообщений
    """
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': auto_offset_reset
    }
    consumer = Consumer(conf)
    
    # Преобразуем topics в список если это строка
    if isinstance(topics, str):
        topics = [topics]
    
    try:
        consumer.subscribe(topics)
        
        start_time = time.time()
        message_count = 0
        processed_ids = set()  # Set для отслеживания обработанных ID
        
        while time.time() - start_time < max_runtime and message_count < max_messages:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error() == KafkaError._PARTITION_EOF:
                    print("Достигнут конец раздела")
                    break
                else:
                    print(f"Error: {msg.error()}")
                    break
            
            # Декодирование сообщения
            try:
                message_value = json.loads(msg.value().decode('utf-8'))
                print(f"Received message: {message_value}")
                
                # Валидация данных если указана модель
                if data_model:
                    validated_data = data_model(**message_value)
                else:
                    validated_data = message_value
                
                # Проверка на дубликаты по ID
                data_id = getattr(validated_data, 'id', None)
                if data_id is not None and data_id in processed_ids:
                    print(f"Дубликат сообщения с ID {data_id}, пропускаем")
                    continue
                
                # Вставка данных если указана функция
                if insert_function:
                    if insert_params:
                        success = insert_function(validated_data, **insert_params)
                    else:
                        success = insert_function(validated_data)
                    
                    # Если вставка успешна, добавляем ID в set
                    if success and data_id is not None:
                        processed_ids.add(data_id)
                        message_count += 1
                        print(f"Обработано сообщений: {message_count}")
                    elif not success:
                        print(f"Не удалось вставить данные с ID {data_id}")
                else:
                    # Если нет функции вставки, просто считаем сообщение обработанным
                    if data_id is not None:
                        processed_ids.add(data_id)
                    message_count += 1
                    print(f"Обработано сообщений: {message_count}")
                
            except Exception as e:
                print(f"Ошибка при обработке сообщения: {e}")
                continue
    
    finally:
        consumer.close()
        print(f"Kafka consumer завершен. Обработано сообщений: {message_count}")
        return message_count


# Примеры использования универсальных функций:

def insert_photo_with_dependency(data: Photo) -> bool:
    """Пример использования универсальной функции для вставки фотографий"""
    return universal_insert_into_postgres(
        data=data,
        table_name="photos",
        schema="dds",
        check_dependency={"users": "user_id"},
        insert_columns=["id", "user_id", "filename", "url", "description", "uploaded_at", "is_private"],
        insert_values=[data.id, data.user_id, data.filename, data.url, data.description, data.uploaded_at, data.is_private]
    )


def consume_media_photos() -> int:
    """Пример использования универсальной функции для потребления фотографий"""
    return universal_kafka_consumer(
        topics="media_photos",
        group_id="sensor_consumer_group",
        data_model=Photo,
        insert_function=insert_photo_with_dependency
    )


def consume_multiple_topics() -> int:
    """Пример потребления из нескольких топиков"""
    return universal_kafka_consumer(
        topics=["media_photos", "media_videos", "media_albums"],
        group_id="multi_media_consumer",
        max_runtime=600,  # 10 минут
        max_messages=500,
        data_model=Photo,  # Можно сделать более гибким
        insert_function=universal_insert_into_postgres,
        insert_params={
            "table_name": "photos",
            "schema": "dds",
            "check_dependency": {"users": "user_id"}
        }
    )
