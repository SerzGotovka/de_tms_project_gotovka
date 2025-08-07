import pandas as pd
import logging
import random
import uuid
import sys
import os
from datetime import datetime, timezone
from utils.function import save_csv_file
from faker import Faker
from typing import List, Dict
from utils.config_generate import NUM_USERS, PRIVACY_LEVELS, STATUS_OPTIONS
from airflow.providers.amazon.aws.hooks.s3 import S3Hook #type: ignore

fake = Faker()


def gen_user(**kwargs) -> List[Dict]:
    """создание структуры user и добавление данных"""
    users = []
    for _ in range(NUM_USERS):
        user = {
            "id": str(uuid.uuid4()),
            "username": fake.unique.user_name(),
            "email": fake.unique.email(),
            "password_hash": fake.sha256(),  # хэширование пароля
            "created_at": fake.date_time_between(start_date="-2y", end_date="now"),
            "updated_at": datetime.now(timezone.utc),
        }
        users.append(user)
    logging.info(f"Сгенерировано пользователей: {len(users)}")
    logging.info(users)


    temp_file_path = f'/opt/airflow/dags/save_data/users/data_users_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'

    #Функция для сохранения сгенерированных файлов в csv
    save_csv_file(temp_file_path, users)

    filename = f'data_users_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'

    
    hook = S3Hook(aws_conn_id='minio_default')
    bucket_name = 'data-bucket'

    if not hook.check_for_bucket(bucket_name):
        hook.create_bucket(bucket_name)

    hook.load_file(

        filename=temp_file_path,
        bucket_name=bucket_name,
        key='/users/' + filename,
        replace=False

    )

    logging

    os.remove(temp_file_path)

    return users


def gen_user_profile(users: List[Dict]) -> List[Dict]:
    """создание структуры user_profiles и добавление данных"""
    profiles = []
    for user in users:
        profile = {
            "user_id": user["id"],
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "bio": fake.text(max_nb_chars=200),
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70),
            "location": fake.city(),
            "updated_at": datetime.now(timezone.utc),
        }
        profiles.append(profile)
    logging.info(f"Сгенерировано профилей пользователей: {len(profiles)}")
    logging.info(profiles)

    temp_file_path = f'/opt/airflow/dags/save_data/users/data_users_profiles_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'
    save_csv_file(temp_file_path, profiles)

    return profiles


def gen_user_settings(users: List[Dict]) -> List[Dict]:
    """создание структуры user_settings и добавление данных"""
    settings = []
    for user in users:
        setting = {
            "user_id": user["id"],
            "language": fake.language_code(),
            "timezone": fake.timezone(),
            "theme": random.choice(["light", "dark", "auto"]),
            "notifications": {
                "email": random.choice([True, False]),
                "push": random.choice([True, False]),
                "marketing": random.choice([True, False]),
            },
            "updated_at": datetime.now(timezone.utc),
        }
        settings.append(setting)
    logging.info(f"Сгенерировано настроек пользователей: {len(settings)}")
    logging.info(settings)

    temp_file_path = f'/opt/airflow/dags/save_data/users/data_users_settings_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    save_csv_file(temp_file_path, settings)

    
    return settings


def gen_user_privacy(users: List[Dict]) -> List[Dict]:
    """создание структуры user_privacy и добавление данных"""
    privacies = []
    for user in users:
        privacy = {
            "user_id": user["id"],
            "profile_visibility": random.choice(PRIVACY_LEVELS),
            "show_email": random.choice([True, False]),
            "show_birth_date": random.choice([True, False]),
            "updated_at": datetime.now(timezone.utc),
        }
        privacies.append(privacy)
    logging.info(f"Сгенерировано настроек приватности: {len(privacies)}")
    logging.info(privacies)

    temp_file_path = f'/opt/airflow/dags/save_data/users/data_users_privacies_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'
    save_csv_file(temp_file_path, privacies)

    return privacies

def gen_user_status(users: List[Dict]) -> List[Dict]:
    """создание структуры user_status и добавление данных"""
    statuses = []
    for user in users:
        status = {
            "user_id": user["id"],
            "status": random.choice(STATUS_OPTIONS),
            "last_seen_at": fake.date_time_between(start_date="-3h", end_date="now"),
        }
        statuses.append(status)
    logging.info(f"Сгенерировано статусов пользователей: {len(statuses)}")
    logging.info(statuses)

    temp_file_path = f'/opt/airflow/dags/save_data/users/data_users_status_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'
    save_csv_file(temp_file_path, statuses)

    return statuses


# if __name__ == "__main__":
#     # Генерируем пользователей
#     users = gen_user()
    
#     # Генерируем профили для пользователей
#     profiles = gen_user_profile(users)
    
#     # Генерируем настройки для пользователей
#     settings = gen_user_settings(users)
    
#     # Генерируем настройки приватности для пользователей
#     privacies = gen_user_privacy(users)
    
#     # Генерируем статусы для пользователей
#     statuses = gen_user_status(users)
    
#     logging.info("\nИтоговая статистика:")
#     logging.info(f"  users: {len(users)} строк")
#     logging.info(f"  user_profiles: {len(profiles)} строк")
#     logging.info(f"  user_settings: {len(settings)} строк")
#     logging.info(f"  user_privacy: {len(privacies)} строк")
#     logging.info(f"  user_status: {len(statuses)} строк")
