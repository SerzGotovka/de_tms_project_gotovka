from typing import Dict, List, Optional, Generator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
import pandas as pd
import logging
import os
import tempfile
from io import StringIO
import psycopg2
from psycopg2.extras import RealDictCursor

def save_csv_file(temp_file_path: str, data:Dict=None) -> None:
    """Функция для сохранения сгенерированных данных в csv"""
    df = pd.DataFrame(data)

    os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)
    logging.info(f"Папка создана: {os.path.dirname(temp_file_path)}")

    df.to_csv(temp_file_path, index=False)
    logging.info(f"Файл создан: {temp_file_path}")


def save_data_directly_to_minio(data: Dict, filename: str, folder: str, bucket_name: str = "data-bucket") -> int:
    """
    Функция для прямой записи сгенерированных данных в MinIO без сохранения в локальные файлы.
    
    Args:
        data (Dict): Словарь с данными для записи
        filename (str): Имя файла в MinIO
        folder (str): Папка в MinIO (например, "/users/", "/groups/")
        bucket_name (str): Имя бакета в MinIO
    
    Returns:
        int: Количество записей в файле
    """
    if not data:
        logging.warning("Получены пустые данные для записи в MinIO")
        return 0
    
    try:
        # Создаем DataFrame из данных
        df = pd.DataFrame(data)
        record_count = len(df)
        
        if record_count == 0:
            logging.warning("DataFrame пуст, пропускаем запись")
            return 0
        
        logging.info(f"Подготавливаю {record_count} записей для записи в MinIO")
        
        # Инициализируем S3Hook
        hook = S3Hook(aws_conn_id="minio_default")
        
        # Проверяем существование бакета, создаем если нужно
        if not hook.check_for_bucket(bucket_name):
            hook.create_bucket(bucket_name)
            logging.info(f"Создан бакет {bucket_name}")
        
        # Формируем ключ для файла в MinIO
        key = folder + filename if folder else filename
        
        # Конвертируем DataFrame в CSV строку
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        # Загружаем данные напрямую в MinIO
        hook.load_string(
            string_data=csv_content,
            key=key,
            bucket_name=bucket_name,
            replace=True
        )
        
        logging.info(f"✅ Успешно записано {record_count} записей в MinIO: {bucket_name}/{key}")
        return record_count
        
    except Exception as e:
        logging.error(f"❌ Ошибка при записи данных в MinIO: {str(e)}")
        raise


def load_file_to_minio(path: str, filename: str, bucket_name: str = "data-bucket", folder:str=None) -> int:
    """Функция для загрузки файлов в MINIO"""

    # Проверяем существование файла перед загрузкой
    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл {path} не найден")

    # Подсчитываем количество записей в CSV файле
    try:
        df = pd.read_csv(path)
        record_count = len(df)
        logging.info(f"Файл {filename} содержит {record_count} записей")
    except Exception as e:
        logging.warning(f"Не удалось подсчитать записи в файле {filename}: {e}")
        record_count = 0

    hook = S3Hook(aws_conn_id="minio_default")
    logging.info("S3Hook инициализирован")

    if not hook.check_for_bucket(bucket_name):
        hook.create_bucket(bucket_name)

    key = folder + filename if folder else filename
    logging.info(f"Загружаю файл {path} в бакет {bucket_name} с ключом {key}")

    # Проверяем, существует ли файл с таким ключом
    if hook.check_for_key(key, bucket_name):
        logging.info(f"Файл с ключом {key} уже существует в бакете {bucket_name}. Будет перезаписан.")

    try:
        hook.load_file(filename=path, bucket_name=bucket_name, key=key, replace=True)
        logging.info(f"Файл {path} успешно загружен в MINIO")
        
        # Удаляем локальный файл после успешной загрузки в MinIO
        try:
            os.remove(path)
            logging.info(f"Локальный файл {path} удален после загрузки в MinIO")
        except Exception as e:
            logging.warning(f"Не удалось удалить локальный файл {path}: {e}")
            
        return record_count
            
    except Exception as e:
        logging.error(f"Ошибка при загрузке файла {path} в MINIO: {str(e)}")
        logging.error(f"Детали ошибки: bucket={bucket_name}, key={key}")
        raise


# def get_csv_files_from_minio(prefix: str, bucket_name: str = "data-bucket") -> List[Dict]:
#     """
#     Функция для получения нескольких CSV файлов из MinIO по префиксу
    
#     Args:
#         prefix (str): Префикс для поиска файлов (например, "users/", "groups/")
#         bucket_name (str): Имя бакета в MinIO (по умолчанию "data-bucket")
    
#     Returns:
#         List[Dict]: Список словарей с данными файлов, каждый содержит:
#             - 'key': ключ файла в MinIO
#             - 'data': содержимое файла как DataFrame
#             - 'size': размер файла в байтах
#     """
#     hook = S3Hook(aws_conn_id="minio_default")
#     logging.info(f"Получаю CSV файлы с префиксом '{prefix}' из бакета '{bucket_name}'")
    
#     # Проверяем существование бакета
#     if not hook.check_for_bucket(bucket_name):
#         raise ValueError(f"Бакет '{bucket_name}' не существует")
    
#     try:
#         # Получаем список ключей объектов по префиксу
#         keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        
#         if not keys:
#             logging.warning(f"Файлы с префиксом '{prefix}' не найдены в бакете '{bucket_name}'")
#             return []
        
#         logging.info(f"Найдено {len(keys)} файлов с префиксом '{prefix}'")
        
#         csv_files = []
        
#         for key in keys:
#             # Проверяем, что это файл (не папка)
#             if not key.endswith('/'):
#                 try:
#                     # Получаем содержимое файла из MinIO
#                     csv_content = hook.read_key(key=key, bucket_name=bucket_name)
                    
#                     # Читаем содержимое как CSV
#                     df = pd.read_csv(StringIO(csv_content))
                    
#                     # Получаем размер файла
#                     file_size = len(csv_content.encode('utf-8'))
                    
#                     csv_files.append({
#                         'key': key,
#                         'data': df,
#                         'size': file_size
#                     })
                    
#                     logging.info(f"Успешно получен файл: {key} (размер: {file_size} байт, записей: {len(df)})")
                    
#                 except Exception as e:
#                     logging.error(f"Ошибка при получении файла {key}: {str(e)}")
#                     continue
        
#         logging.info(f"Успешно получено {len(csv_files)} CSV файлов")
#         return csv_files
        
#     except Exception as e:
#         logging.error(f"Ошибка при получении файлов из MinIO: {str(e)}")
        raise
#########################################################

def read_csv_files_from_minio(prefix: str, bucket_name: str = "data-bucket") -> Generator[pd.DataFrame, None, None]:
    """
    Функция для поочередного чтения CSV файлов из MinIO по префиксу.
    Возвращает генератор DataFrame'ов для экономии памяти.
    
    Args:
        prefix (str): Префикс для поиска файлов (например, "users/", "groups/")
        bucket_name (str): Имя бакета в MinIO (по умолчанию "data-bucket")
    
    Yields:
        pd.DataFrame: DataFrame с данными из очередного CSV файла
    """
    hook = S3Hook(aws_conn_id="minio_default")
    logging.info(f"Начинаю чтение CSV файлов с префиксом '{prefix}' из бакета '{bucket_name}'")
    
    # Проверяем существование бакета
    if not hook.check_for_bucket(bucket_name):
        raise ValueError(f"Бакет '{bucket_name}' не существует")
    
    try:
        # Получаем список ключей объектов по префиксу
        keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        
        if not keys:
            logging.warning(f"Файлы с префиксом '{prefix}' не найдены в бакете '{bucket_name}'")
            return
        
        logging.info(f"Найдено {len(keys)} файлов с префиксом '{prefix}'")
        
        for key in keys:
            # Проверяем, что это файл (не папка)
            if not key.endswith('/'):
                try:
                    # Получаем содержимое файла из MinIO
                    csv_content = hook.read_key(key=key, bucket_name=bucket_name)
                    
                    # Читаем содержимое как CSV
                    df = pd.read_csv(StringIO(csv_content))
                    
                    logging.info(f"Успешно прочитан файл: {key} (записей: {len(df)})")
                    yield df
                    
                except Exception as e:
                    logging.error(f"Ошибка при чтении файла {key}: {str(e)}")
                    continue
        
        logging.info(f"Завершено чтение файлов с префиксом '{prefix}'")
        
    except Exception as e:
        logging.error(f"Ошибка при получении файлов из MinIO: {str(e)}")
        raise


def parse_badges_string(badges_str: str) -> list:
    """
    Парсит строку badges в список.
    
    Args:
        badges_str (str): Строка с badges (например, "['badge1', 'badge2']" или "[]")
    
    Returns:
        list: Список badges
    """
    if not badges_str or badges_str.strip() == '':
        return []
    
    try:
        # Убираем лишние пробелы и кавычки
        badges_str = badges_str.strip()
        
        # Если это пустой список
        if badges_str == '[]':
            return []
        
        # Если это строка вида "['badge1', 'badge2']"
        if badges_str.startswith('[') and badges_str.endswith(']'):
            # Убираем квадратные скобки
            badges_str = badges_str[1:-1]
            
            # Разделяем по запятым и очищаем от кавычек
            badges = []
            for badge in badges_str.split(','):
                badge = badge.strip().strip("'\"")
                if badge:
                    badges.append(badge)
            return badges
        
        # Если это просто строка без скобок, разделяем по запятым
        badges = []
        for badge in badges_str.split(','):
            badge = badge.strip().strip("'\"")
            if badge:
                badges.append(badge)
        return badges
        
    except Exception as e:
        logging.warning(f"Ошибка при парсинге badges '{badges_str}': {e}")
        return []


def parse_media_ids_string(media_ids_str: str) -> list:
    """
    Парсит строку media_ids в список UUID.
    
    Args:
        media_ids_str (str): Строка с media_ids (например, "['uuid1', 'uuid2']" или "[]")
    
    Returns:
        list: Список UUID строк
    """
    if not media_ids_str or media_ids_str.strip() == '':
        return []
    
    try:
        # Убираем лишние пробелы и кавычки
        media_ids_str = media_ids_str.strip()
        
        # Если это пустой список
        if media_ids_str == '[]':
            return []
        
        # Если это строка вида "['uuid1', 'uuid2']"
        if media_ids_str.startswith('[') and media_ids_str.endswith(']'):
            # Убираем квадратные скобки
            media_ids_str = media_ids_str[1:-1]
            
            # Разделяем по запятым и очищаем от кавычек
            media_ids = []
            for media_id in media_ids_str.split(','):
                media_id = media_id.strip().strip("'\"")
                if media_id:
                    media_ids.append(media_id)
            return media_ids
        
        # Если это просто строка без скобок, разделяем по запятым
        media_ids = []
        for media_id in media_ids_str.split(','):
            media_id = media_id.strip().strip("'\"")
            if media_id:
                media_ids.append(media_id)
        return media_ids
        
    except Exception as e:
        logging.warning(f"Ошибка при парсинге media_ids '{media_ids_str}': {e}")
        return []


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Функция для очистки DataFrame от дубликатов и null значений.
    Удаляет всю запись, если в ней есть хотя бы одно null значение или дубликат.
    
    Args:
        df (pd.DataFrame): Исходный DataFrame для очистки
    
    Returns:
        pd.DataFrame: Очищенный DataFrame без дубликатов и null значений
    """
    if df.empty:
        logging.warning("Получен пустой DataFrame для очистки")
        return df
    
    original_count = len(df)
    logging.info(f"Начинаю очистку DataFrame. Исходное количество записей: {original_count}")
    
    # Удаляем записи с null значениями
    df_cleaned = df.dropna()
    after_null_removal = len(df_cleaned)
    null_removed = original_count - after_null_removal
    
    if null_removed > 0:
        logging.info(f"Удалено {null_removed} записей с null значениями")
    
    # Удаляем дубликаты
    df_cleaned = df_cleaned.drop_duplicates()
    final_count = len(df_cleaned)
    duplicates_removed = after_null_removal - final_count
    
    if duplicates_removed > 0:
        logging.info(f"Удалено {duplicates_removed} дубликатов")
    
    total_removed = original_count - final_count
    logging.info(f"Очистка завершена. Удалено записей: {total_removed}, осталось: {final_count}")
    
    return df_cleaned


def validate_foreign_keys(df: pd.DataFrame, table_name: str, postgres_conn_id: str = "my_postgres_conn") -> pd.DataFrame:
    """
    Функция для проверки внешних ключей перед вставкой данных.
    Удаляет записи с несуществующими внешними ключами.
    
    Args:
        df (pd.DataFrame): DataFrame с данными для проверки
        table_name (str): Имя таблицы для определения внешних ключей
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        pd.DataFrame: DataFrame с валидными внешними ключами
    """
    if df.empty:
        return df
    
    # Определяем внешние ключи для каждой таблицы
    foreign_key_mappings = {
        'user_profiles': {'user_id': 'users'},
        'user_settings': {'user_id': 'users'},
        'user_privacy': {'user_id': 'users'},
        'user_status': {'user_id': 'users'},
        'friends': {'user_id': 'users', 'friend_id': 'users'},
        'followers': {'user_id': 'users', 'follower_id': 'users'},
        'subscriptions': {'user_id': 'users', 'subscribed_to': 'users'},
        'blocks': {'user_id': 'users', 'blocked_id': 'users'},
        'mutes': {'user_id': 'users', 'muted_id': 'users'},
        'close_friends': {'user_id': 'users', 'close_friend_id': 'users'},
        'posts': {'author_id': 'users'},
        'stories': {'user_id': 'users'},
        'reels': {'user_id': 'users'},
        'comments': {'user_id': 'users', 'post_id': 'posts'},
        'replies': {'user_id': 'users', 'comment_id': 'comments'},
        'group_members': {'user_id': 'users', 'group_id': 'groups'},
    }
    
    if table_name not in foreign_key_mappings:
        logging.info(f"Внешние ключи для таблицы {table_name} не определены, пропускаем проверку")
        return df
    
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = None
    cursor = None
    
    try:
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        original_count = len(df)
        valid_df = df.copy()
        
        # Проверяем каждый внешний ключ
        for fk_column, ref_table in foreign_key_mappings[table_name].items():
            if fk_column not in df.columns:
                continue
                
            logging.info(f"Проверяю внешний ключ {fk_column} -> {ref_table}")
            
            # Получаем уникальные значения внешнего ключа
            unique_fk_values = df[fk_column].dropna().unique()
            if len(unique_fk_values) == 0:
                continue
            
            # Проверяем существование в справочной таблице
            placeholders = ', '.join(['%s'] * len(unique_fk_values))
            query = f"SELECT id FROM dds.{ref_table} WHERE id IN ({placeholders})"
            cursor.execute(query, list(unique_fk_values))
            existing_ids = {row[0] for row in cursor.fetchall()}
            
            # Фильтруем записи с несуществующими внешними ключами
            before_count = len(valid_df)
            valid_df = valid_df[valid_df[fk_column].isin(existing_ids)]
            after_count = len(valid_df)
            
            removed_count = before_count - after_count
            if removed_count > 0:
                logging.warning(f"Удалено {removed_count} записей с несуществующим {fk_column}")
        
        final_count = len(valid_df)
        total_removed = original_count - final_count
        
        if total_removed > 0:
            logging.warning(f"Валидация внешних ключей завершена. Удалено записей: {total_removed}, осталось: {final_count}")
        else:
            logging.info(f"Все внешние ключи валидны. Обработано записей: {final_count}")
        
        return valid_df
        
    except Exception as e:
        logging.error(f"Ошибка при валидации внешних ключей: {e}")
        return df
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def save_dataframe_to_postgres(
    df: pd.DataFrame, 
    table_name: str, 
    pydantic_model_class,
    postgres_conn_id: str = "my_postgres_conn",
    update_existing: bool = False,
    batch_size: int = 1000
) -> int:
    """
    Функция для записи очищенных данных в PostgreSQL через pydantic модели
    с проверкой существования записей.
    
    Args:
        df (pd.DataFrame): DataFrame с данными для записи
        table_name (str): Имя таблицы в PostgreSQL
        pydantic_model_class: Класс pydantic модели для валидации
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
        update_existing (bool): Если True, обновляет существующие записи, иначе пропускает
        batch_size (int): Размер батча для обработки записей
    
    Returns:
        int: Количество успешно обработанных записей
    """
    if df.empty:
        logging.warning("Получен пустой DataFrame для записи в PostgreSQL")
        return 0
    
    # Валидируем внешние ключи перед обработкой
    df = validate_foreign_keys(df, table_name, postgres_conn_id)
    
    if df.empty:
        logging.warning("После валидации внешних ключей DataFrame стал пустым")
        return 0
    
    logging.info(f"Начинаю запись {len(df)} записей в таблицу {table_name}")
    logging.info(f"Режим обновления существующих записей: {update_existing}")
    
    # Используем PostgresHook для подключения через Airflow connection
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = None
    cursor = None
    inserted_count = 0
    updated_count = 0
    
    try:
        # Получаем подключение через PostgresHook
        conn = postgres_hook.get_conn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Получаем список существующих ID для проверки дубликатов (только ID, не все данные)
        cursor.execute(f"SELECT id FROM dds.{table_name}")
        existing_ids = {row['id'] for row in cursor.fetchall()}
        
        logging.info(f"Найдено {len(existing_ids)} существующих записей в таблице {table_name}")
        
        # Создаем set для отслеживания ID в текущем батче для избежания дубликатов
        current_batch_ids = set()
        
        # Обрабатываем данные батчами
        for batch_start in range(0, len(df), batch_size):
            batch_end = min(batch_start + batch_size, len(df))
            batch_df = df.iloc[batch_start:batch_end]
            
            logging.info(f"Обрабатываю батч {batch_start}-{batch_end} из {len(df)} записей")
            
            # Очищаем set для нового батча
            current_batch_ids.clear()
            
            # Обрабатываем каждую запись в батче
            for index, row in batch_df.iterrows():
                try:
                    # Конвертируем row в словарь
                    row_dict = row.to_dict()
                    
                    # Обрабатываем поле badges если оно есть
                    if 'badges' in row_dict and isinstance(row_dict['badges'], str):
                        row_dict['badges'] = parse_badges_string(row_dict['badges'])
                    
                    # Обрабатываем поле media_ids если оно есть
                    if 'media_ids' in row_dict and isinstance(row_dict['media_ids'], str):
                        row_dict['media_ids'] = parse_media_ids_string(row_dict['media_ids'])
                    
                    # Валидируем данные через pydantic модель
                    validated_data = pydantic_model_class(**row_dict)
                    
                    # Проверяем дубликаты в текущем батче
                    if validated_data.id in current_batch_ids:
                        logging.debug(f"Дубликат в текущем батче: ID {validated_data.id}, пропускаем")
                        continue
                    
                    # Проверяем, существует ли запись с таким ID в базе данных
                    if validated_data.id in existing_ids:
                        if update_existing:
                            # Обновляем существующую запись
                            insert_data = validated_data.dict()
                            update_columns = [col for col in insert_data.keys() if col != 'id']
                            update_placeholders = ', '.join([f"{col} = %s" for col in update_columns])
                            update_values = [insert_data[col] for col in update_columns] + [insert_data['id']]
                            
                            sql = f"""
                                UPDATE dds.{table_name} 
                                SET {update_placeholders}
                                WHERE id = %s
                            """
                            
                            cursor.execute(sql, update_values)
                            updated_count += 1
                            logging.debug(f"Обновлена запись с ID {validated_data.id}")
                        else:
                            logging.debug(f"Запись с ID {validated_data.id} уже существует в БД, пропускаем")
                            continue
                    else:
                        # Вставляем новую запись
                        insert_data = validated_data.dict()
                        columns = list(insert_data.keys())
                        placeholders = ', '.join(['%s'] * len(columns))
                        columns_str = ', '.join(columns)
                        
                        sql = f"""
                            INSERT INTO dds.{table_name} ({columns_str})
                            VALUES ({placeholders})
                        """
                        
                        cursor.execute(sql, list(insert_data.values()))
                        inserted_count += 1
                        logging.debug(f"Вставлена новая запись с ID {validated_data.id}")
                        
                        # Добавляем ID в множества для отслеживания
                        existing_ids.add(validated_data.id)
                        current_batch_ids.add(validated_data.id)
                    
                except Exception as e:
                    logging.error(f"Ошибка при обработке записи {index}: {str(e)}")
                    continue
            
            # Подтверждаем транзакцию после каждого батча
            conn.commit()
            logging.info(f"Батч {batch_start}-{batch_end} обработан. Вставлено: {inserted_count}, обновлено: {updated_count}")
        
        total_processed = inserted_count + updated_count
        logging.info(f"Загрузка завершена. Всего обработано записей: {total_processed} (вставлено: {inserted_count}, обновлено: {updated_count})")
        
        return total_processed
        
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Ошибка при записи в PostgreSQL: {str(e)}")
        raise
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def load_from_raw_to_dds(
    prefix: str, 
    table_name: str, 
    pydantic_model_class,
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn",
    update_existing: bool = False,
    batch_size: int = 1000,
    move_to_complete: bool = True
) -> int:
    """
    Функция для загрузки данных из raw слоя (MinIO) в DDS слой (PostgreSQL).
    Читает CSV файлы из MinIO, очищает их и сохраняет в PostgreSQL.
    После успешной записи в PostgreSQL переносит файлы в бакет complete.
    
    Args:
        prefix (str): Префикс для поиска файлов в MinIO (например, "users/", "groups/")
        table_name (str): Имя таблицы в PostgreSQL для записи данных
        pydantic_model_class: Класс pydantic модели для валидации данных
        bucket_name (str): Имя бакета в MinIO (по умолчанию "data-bucket")
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
        update_existing (bool): Если True, обновляет существующие записи, иначе пропускает
        batch_size (int): Размер батча для обработки записей
        move_to_complete (bool): Если True, переносит файлы в бакет complete после успешной записи
    
    Returns:
        int: Общее количество успешно загруженных записей
    """
    logging.info(f"Начинаю загрузку данных из raw слоя в DDS слой")
    logging.info(f"Префикс: {prefix}, Таблица: {table_name}, Бакет: {bucket_name}")
    
    total_inserted = 0
    
    try:
        # Читаем CSV файлы из MinIO по префиксу
        for df in read_csv_files_from_minio(prefix=prefix, bucket_name=bucket_name):
            if df.empty:
                logging.warning("Получен пустой DataFrame, пропускаем")
                continue
            
            logging.info(f"Обрабатываю DataFrame с {len(df)} записями")
            
            # Очищаем DataFrame от дубликатов и null значений
            df_cleaned = clean_dataframe(df)
            
            if df_cleaned.empty:
                logging.warning("После очистки DataFrame стал пустым, пропускаем")
                continue
            
            # Сохраняем очищенные данные в PostgreSQL
            inserted_count = save_dataframe_to_postgres(
                df=df_cleaned,
                table_name=table_name,
                pydantic_model_class=pydantic_model_class,
                postgres_conn_id=postgres_conn_id,
                update_existing=update_existing,
                batch_size=batch_size
            )
            
            total_inserted += inserted_count
            logging.info(f"Загружено {inserted_count} записей в таблицу {table_name}")
    
    except Exception as e:
        logging.error(f"Ошибка при загрузке данных из raw в DDS: {str(e)}")
        raise
    
    # Переносим файлы в бакет complete после успешной записи в PostgreSQL
    if move_to_complete and total_inserted > 0:
        try:
            moved_files = move_files_to_complete_bucket(
                prefix=prefix,
                source_bucket=bucket_name,
                complete_bucket="complete"
            )
            logging.info(f"Перенесено {moved_files} файлов в бакет complete")
        except Exception as e:
            logging.error(f"Ошибка при переносе файлов в бакет complete: {str(e)}")
            # Не прерываем выполнение, так как данные уже записаны в PostgreSQL
    
    logging.info(f"Загрузка завершена. Всего загружено записей: {total_inserted}")
    return total_inserted


# ==================== GROUPS DATA FUNCTIONS ====================

def load_communities_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных сообществ из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.groups_pydantic import Community
    
    return load_from_raw_to_dds(
        prefix="communities/",
        table_name="communities",
        pydantic_model_class=Community,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def load_groups_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных групп из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.groups_pydantic import Group
    
    return load_from_raw_to_dds(
        prefix="groups/",
        table_name="groups",
        pydantic_model_class=Group,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def load_group_members_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных участников групп из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.groups_pydantic import GroupMember
    
    return load_from_raw_to_dds(
        prefix="group_members/",
        table_name="group_members",
        pydantic_model_class=GroupMember,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def load_community_topics_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных тем сообществ из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.groups_pydantic import CommunityTopic
    
    return load_from_raw_to_dds(
        prefix="community_topics/",
        table_name="community_topics",
        pydantic_model_class=CommunityTopic,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def load_pinned_posts_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных закрепленных постов из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.groups_pydantic import PinnedPost
    
    return load_from_raw_to_dds(
        prefix="pinned_posts/",
        table_name="pinned_posts",
        pydantic_model_class=PinnedPost,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


# ==================== CONTENT DATA FUNCTIONS ====================

def load_posts_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных постов из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.pydantic_models import Post
    
    return load_from_raw_to_dds(
        prefix="posts/",
        table_name="posts",
        pydantic_model_class=Post,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def load_stories_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных историй из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.pydantic_models import Story
    
    return load_from_raw_to_dds(
        prefix="stories/",
        table_name="stories",
        pydantic_model_class=Story,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def load_reels_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных Reels из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.pydantic_models import Reel
    
    return load_from_raw_to_dds(
        prefix="reels/",
        table_name="reels",
        pydantic_model_class=Reel,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def load_comments_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных комментариев из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.pydantic_models import Comment
    
    return load_from_raw_to_dds(
        prefix="comments/",
        table_name="comments",
        pydantic_model_class=Comment,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def load_replies_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных ответов на комментарии из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.pydantic_models import Reply
    
    return load_from_raw_to_dds(
        prefix="replies/",
        table_name="replies",
        pydantic_model_class=Reply,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def load_likes_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных лайков из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.pydantic_models import Like
    
    return load_from_raw_to_dds(
        prefix="likes/",
        table_name="likes",
        pydantic_model_class=Like,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def load_reactions_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных реакций из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.pydantic_models import Reaction
    
    return load_from_raw_to_dds(
        prefix="reactions/",
        table_name="reactions",
        pydantic_model_class=Reaction,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def load_shares_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных репостов из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.pydantic_models import Share
    
    return load_from_raw_to_dds(
        prefix="shares/",
        table_name="shares",
        pydantic_model_class=Share,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def move_files_to_complete_bucket(
    prefix: str, 
    source_bucket: str = "data-bucket", 
    complete_bucket: str = "complete"
) -> int:
    """
    Функция для переноса файлов из исходного бакета в бакет complete после успешной обработки.
    
    Args:
        prefix (str): Префикс для поиска файлов (например, "users/", "groups/")
        source_bucket (str): Имя исходного бакета в MinIO
        complete_bucket (str): Имя бакета для завершенных файлов
    
    Returns:
        int: Количество перенесенных файлов
    """
    hook = S3Hook(aws_conn_id="minio_default")
    logging.info(f"Начинаю перенос файлов с префиксом '{prefix}' из бакета '{source_bucket}' в '{complete_bucket}'")
    
    # Проверяем существование исходного бакета
    if not hook.check_for_bucket(source_bucket):
        logging.warning(f"Исходный бакет '{source_bucket}' не существует")
        return 0
    
    # Создаем бакет complete если он не существует
    if not hook.check_for_bucket(complete_bucket):
        hook.create_bucket(complete_bucket)
        logging.info(f"Создан бакет '{complete_bucket}'")
    
    try:
        # Получаем список ключей объектов по префиксу
        keys = hook.list_keys(bucket_name=source_bucket, prefix=prefix)
        
        if not keys:
            logging.warning(f"Файлы с префиксом '{prefix}' не найдены в бакете '{source_bucket}'")
            return 0
        
        logging.info(f"Найдено {len(keys)} файлов с префиксом '{prefix}' для переноса")
        
        moved_count = 0
        
        for key in keys:
            # Проверяем, что это файл (не папка)
            if not key.endswith('/'):
                try:
                    # Копируем файл в бакет complete
                    hook.copy_object(
                        source_bucket_key=key,
                        dest_bucket_key=key,
                        source_bucket_name=source_bucket,
                        dest_bucket_name=complete_bucket
                    )
                    
                    # Удаляем файл из исходного бакета
                    hook.delete_objects(bucket=source_bucket, keys=[key])
                    
                    moved_count += 1
                    logging.info(f"Файл {key} успешно перенесен в бакет {complete_bucket}")
                    
                except Exception as e:
                    logging.error(f"Ошибка при переносе файла {key}: {str(e)}")
                    continue
        
        logging.info(f"Перенос завершен. Перенесено файлов: {moved_count}")
        return moved_count
        
    except Exception as e:
        logging.error(f"Ошибка при переносе файлов: {str(e)}")
        raise


# ==================== MEDIA DATA FUNCTIONS ====================

def load_photos_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных фотографий из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.pydantic_models import Photo
    
    return load_from_raw_to_dds(
        prefix="photos/",
        table_name="photos",
        pydantic_model_class=Photo,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def load_videos_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных видео из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.pydantic_models import Video
    
    return load_from_raw_to_dds(
        prefix="videos/",
        table_name="videos",
        pydantic_model_class=Video,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )


def load_albums_from_minio_to_postgres(
    bucket_name: str = "data-bucket",
    postgres_conn_id: str = "my_postgres_conn"
) -> int:
    """
    Функция для загрузки данных альбомов из MinIO в PostgreSQL.
    
    Args:
        bucket_name (str): Имя бакета в MinIO
        postgres_conn_id (str): ID подключения к PostgreSQL в Airflow
    
    Returns:
        int: Количество загруженных записей
    """
    from models.pydantic.pydantic_models import Album
    
    return load_from_raw_to_dds(
        prefix="albums/",
        table_name="albums",
        pydantic_model_class=Album,
        bucket_name=bucket_name,
        postgres_conn_id=postgres_conn_id
    )



