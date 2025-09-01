from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
from typing import List, Dict
from utils.config_generate import NUM_USERS, BADGE_NAMES, PRIVACY_LEVELS, STATUS_OPTIONS
import logging

fake = Faker()


def gen_user(n=NUM_USERS) -> List[Dict]:
    """Генерация пользователей"""
    users = []
    for i in range(n):
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
    logging.info(f"Сгенерировано пользователей: {len(users)}")
    logging.info(users)
    return users


def gen_user_profile(users: List[Dict]) -> List[Dict]:
    """Генерация профилей пользователей"""
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
    logging.info(f"Сгенерировано профилей: {len(profiles)}")
    logging.info(profiles)
    return profiles


def gen_user_settings(users: List[Dict]) -> List[Dict]:
    """Генерация настроек пользователей"""
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
    logging.info(f"Сгенерировано настроек: {len(settings)}")
    logging.info(settings)
    return settings


def gen_user_privacy(users: List[Dict]) -> List[Dict]:
    """Генерация настроек приватности пользователей"""
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
    logging.info(f"Сгенерировано настроек приватности: {len(privacies)}")
    logging.info(privacies)
    return privacies


def gen_user_status(users: List[Dict]) -> List[Dict]:
    """Генерация статусов пользователей"""
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
    logging.info(f"Сгенерировано статусов: {len(statuses)}")
    logging.info(statuses)
    return statuses
