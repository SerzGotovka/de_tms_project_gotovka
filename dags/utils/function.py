from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore
import pandas as pd
import logging
import os


def save_csv_file(temp_file_path: str, data=None) -> None:
    """Функция для сохранения сгенерированных данных в csv"""
    df = pd.DataFrame(data)

    os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)
    logging.info(f"Папка создана: {os.path.dirname(temp_file_path)}")

    df.to_csv(temp_file_path, index=False)
    logging.info(f"Файл создан: {temp_file_path}")


def load_file_to_minio(path: str, filename: str, bucket_name: str = "data-bucket", folder:str=None) -> None:
    """Функция для загрузки файлов в MINIO"""

    # Проверяем существование файла перед загрузкой
    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл {path} не найден")

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
        
        # Удаляем временный файл только после успешной загрузки
        try:
            os.remove(path)
            logging.info(f"Временный файл {path} удален")
        except Exception as e:
            logging.warning(f"Не удалось удалить временный файл {path}: {str(e)}")
            
    except Exception as e:
        logging.error(f"Ошибка при загрузке файла {path} в MINIO: {str(e)}")
        logging.error(f"Детали ошибки: bucket={bucket_name}, key={key}")
        raise
