import pandas as pd
import logging
import random
import uuid
import sys
import os
from datetime import datetime, timezone
from utils.function import load_file_to_minio, save_csv_file
from faker import Faker
from typing import List, Dict
from utils.config_generate import NUM_USERS, PRIVACY_LEVELS, STATUS_OPTIONS

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config_generate import temp_file_path_users, filename_users

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
    # Убираем логирование всего списка users, так как он может быть очень большим

 
    #Функция для сохранения сгенерированных файлов в csv
    save_csv_file(temp_file_path_users, users)

    
    load_file_to_minio(temp_file_path_users, filename_users, folder="/users/")
    
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

    temp_file_path = '/opt/airflow/dags/save_data/users/data_users_profiles.csv'
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
            "notifications_email": random.choice([True, False]),
            "notifications_push": random.choice([True, False]),
            "notifications_marketing": random.choice([True, False]),
            "updated_at": datetime.now(timezone.utc),
        }
        settings.append(setting)
    logging.info(f"Сгенерировано настроек пользователей: {len(settings)}")

    temp_file_path = '/opt/airflow/dags/save_data/users/data_users_settings.csv'
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

    temp_file_path = '/opt/airflow/dags/save_data/users/data_users_privacies.csv'
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

    temp_file_path = '/opt/airflow/dags/save_data/users/data_users_status.csv'
    save_csv_file(temp_file_path, statuses)

    return statuses
