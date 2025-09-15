from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
from typing import List, Dict
from utils.config_generate import NUM_USERS, BADGE_NAMES, PRIVACY_LEVELS, STATUS_OPTIONS
import logging

from utils.function_minio import save_csv_file, save_data_directly_to_minio
from utils.config_generate import (temp_file_path_users, temp_file_path_profiles,
                                   temp_file_path_user_settings, temp_file_path_user_privacy,
                                   temp_file_path_user_status, filename_users, filename_users_profiles)
fake = Faker()


def gen_user(n=NUM_USERS, **context) -> List[Dict]:
    """Генерация пользователей"""
    users = []
    for _ in range(n):
        record = {
            "id": str(uuid.uuid4()),
            "username": fake.user_name(),
            "email": fake.email(),
            "password_hash": fake.sha256(),
            "created_at": fake.date_between(
                start_date="-2y", end_date="today"
            ).strftime("%Y.%m.%d %H:%M"),
            "last_login": fake.date_between(
                start_date="-1m", end_date="today"
            ).strftime("%Y.%m.%d %H:%M"),
            "is_active": random.choice([True, False]),
            "is_verified": random.choice([True, False]),
        }
        users.append(record)
    num_users = len(users)
    logging.info(f"Сгенерировано пользователей: {num_users}")
    logging.info(users)
    
    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_users_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=users,
            filename=filename,
            folder="/users/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} пользователей в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи пользователей в MinIO: {e}")
        raise
    
    # Сохраняем данные пользователей в XCom
    context["task_instance"].xcom_push(key="users", value=users)
    # Сохраняем количество пользователей в XCom
    context["task_instance"].xcom_push(key="num_users", value=num_users)
    
    return users


def gen_user_profile(**context) -> List[Dict]:
    """Генерация профилей пользователей"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    profiles = []
    for user in users:
        record = {
            "id": str(uuid.uuid4()),
            "user_id": user["id"],
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "bio": fake.text(max_nb_chars=200),
            "avatar_url": fake.image_url(),
            "cover_photo_url": fake.image_url(),
            "location": fake.city(),
            "website": fake.url(),
            "phone": fake.phone_number(),
            "birth_date": fake.date_of_birth(minimum_age=13, maximum_age=80).strftime("%Y.%m.%d"),
            "gender": random.choice(["male", "female", "other", "prefer_not_to_say"]),
            "badges": random.sample(BADGE_NAMES, random.randint(0, 3)),
            "updated_at": fake.date_between(
                start_date="-6m", end_date="today"
            ).strftime("%Y.%m.%d %H:%M"),
        }
        profiles.append(record)
    num_profiles = len(profiles)
    logging.info(f"Сгенерировано профилей: {len(profiles)}")
    logging.info(profiles)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_users_profiles_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=profiles,
            filename=filename,
            folder="/users_profiles/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} профилей в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи профилей в MinIO: {e}")
        raise
    
    # profiles = gen_user_profile(users)
    context["task_instance"].xcom_push(key="profiles", value=profiles)

    context["task_instance"].xcom_push(key="num_profiles", value=num_profiles)

    return profiles


def gen_user_settings(**context) -> List[Dict]:
    """Генерация настроек пользователей"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    settings = []
    for user in users:
        record = {
            "id": str(uuid.uuid4()),
            "user_id": user["id"],
            "language": random.choice(["en", "ru", "es", "fr", "de"]),
            "timezone": random.choice(["UTC", "UTC+1", "UTC+2", "UTC+3", "UTC-5", "UTC-8"]),
            "theme": random.choice(["light", "dark", "auto"]),
            "notifications_enabled": random.choice([True, False]),
            "email_notifications": random.choice([True, False]),
            "push_notifications": random.choice([True, False]),
            "two_factor_enabled": random.choice([True, False]),
            "auto_save_drafts": random.choice([True, False]),
            "created_at": fake.date_between(
                start_date="-1y", end_date="today"
            ).strftime("%Y.%m.%d %H:%M"),
            "updated_at": fake.date_between(
                start_date="-1m", end_date="today"
            ).strftime("%Y.%m.%d %H:%M"),
        }
        settings.append(record)
    num_settings = len(settings)
    logging.info(f"Сгенерировано настроек: {len(settings)}")
    logging.info(settings)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_users_settings_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=settings,
            filename=filename,
            folder="/users_settings/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} настроек в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи настроек в MinIO: {e}")
        raise

    # settings = gen_user_settings(users)
    context["task_instance"].xcom_push(key="settings", value=settings)
    context["task_instance"].xcom_push(key="num_settings", value=num_settings)
    
    return settings


def gen_user_privacy(**context) -> List[Dict]:
    """Генерация настроек приватности пользователей"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    privacies = []
    for user in users:
        record = {
            "id": str(uuid.uuid4()),
            "user_id": user["id"],
            "profile_visibility": random.choice(PRIVACY_LEVELS),
            "posts_visibility": random.choice(PRIVACY_LEVELS),
            "friends_visibility": random.choice(PRIVACY_LEVELS),
            "photos_visibility": random.choice(PRIVACY_LEVELS),
            "contact_info_visibility": random.choice(PRIVACY_LEVELS),
            "search_visibility": random.choice([True, False]),
            "allow_friend_requests": random.choice([True, False]),
            "allow_messages": random.choice([True, False]),
            "show_online_status": random.choice([True, False]),
            "created_at": fake.date_between(
                start_date="-1y", end_date="today"
            ).strftime("%Y.%m.%d %H:%M"),
            "updated_at": fake.date_between(
                start_date="-1m", end_date="today"
            ).strftime("%Y.%m.%d %H:%M"),
        }
        privacies.append(record)
    num_privacies = len(privacies)
    logging.info(f"Сгенерировано настроек приватности: {len(privacies)}")
    logging.info(privacies)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_users_privacy_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=privacies,
            filename=filename,
            folder="/users_privacy/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} настроек приватности в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи настроек приватности в MinIO: {e}")
        raise
    
    # privacies = gen_user_privacy(users)
    context["task_instance"].xcom_push(key="privacies", value=privacies)
    context["task_instance"].xcom_push(key="num_privacies", value=num_privacies)
    
    return privacies


def gen_user_status(**context) -> List[Dict]:
    """Генерация статусов пользователей"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    statuses = []
    for user in users:
        record = {
            "id": str(uuid.uuid4()),
            "user_id": user["id"],
            "status": random.choice(STATUS_OPTIONS),
            "status_message": fake.sentence(nb_words=5) if random.choice([True, False]) else None,
            "last_seen": fake.date_between(
                start_date="-1d", end_date="today"
            ).strftime("%Y.%m.%d %H:%M"),
            "is_online": random.choice([True, False]),
            "created_at": fake.date_between(
                start_date="-1y", end_date="today"
            ).strftime("%Y.%m.%d %H:%M"),
            "updated_at": fake.date_between(
                start_date="-1h", end_date="today"
            ).strftime("%Y.%m.%d %H:%M"),
        }
        statuses.append(record)
    num_statuses = len(statuses)
    logging.info(f"Сгенерировано статусов: {len(statuses)}")
    logging.info(statuses)
    
    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_users_status_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=statuses,
            filename=filename,
            folder="/users_status/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} статусов в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи статусов в MinIO: {e}")
        raise
    
    # statuses = gen_user_status(users)
    context["task_instance"].xcom_push(key="statuses", value=statuses)
    context["task_instance"].xcom_push(key="num_statuses", value=num_statuses)
    
    return statuses
