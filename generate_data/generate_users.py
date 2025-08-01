import random
import uuid
from datetime import datetime, timedelta, timezone
from faker import Faker
from typing import List, Dict

fake = Faker()

# # Настройки
NUM_USERS = 100  # Количество пользователей

BADGE_NAMES = (
    "verified",
    "top-contributor",
    "beta-tester",
    "early-adopter",
    "vip",
    "moderator",
    "donator",
)
PRIVACY_LEVELS = ("public", "friends", "private")
STATUS_OPTIONS = ("online", "offline")

# ---------- Генераторы по таблицам ----------


def gen_user() -> List[Dict]:
    """Генерация user"""
    users = []
    for _ in range(1, NUM_USERS + 1):
        record = {
            "id": str(uuid.uuid4()),
            "username": fake.unique.user_name(),
            "email": fake.unique.email(),
            "password_hash": fake.sha256(),  # хэширвоание пароля
            "created_at": datetime.now(timezone.utc).strftime("%Y.%m.%d %H:%M"),
        }

        users.append(record)
    return users


def gen_user_profiles(users: List[Dict]) -> List[Dict]:
    """Возвращает список профилей для переданных пользователей."""
    user_profiles = []
    for user in users:
        record = {
            "user_id": user["id"],
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "bio": fake.text(max_nb_chars=200),
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70),
            "location": fake.city(),
            "avatar_url": (
                fake.image_url(width=400, height=400) if random.random() < 0.8 else None
            ),
            "updated_at": datetime.now(timezone.utc).strftime("%Y.%m.%d %H:%M"),
        }
        user_profiles.append(record)
    return user_profiles


def gen_user_settings(users: List[Dict]) -> List[Dict]:
    """Генерация user_settings"""
    user_settings = []
    for user in users:
        record = {
            "user_id": user["id"],
            "language": fake.language_code(),
            "timezone": fake.timezone(),
            "theme": random.choice(["light", "dark", "auto"]),
            "notifications": {
                "email": random.choice([True, False]),
                "push": random.choice([True, False]),
                "marketing": random.choice([True, False]),
            },
            "updated_at": datetime.now(timezone.utc).strftime("%Y.%m.%d %H:%M"),
        }
        user_settings.append(record)
    return user_settings


def gen_user_privacy(users: List[Dict]) -> List[Dict]:
    """Генерация user_privacy"""
    user_privacy = []
    for user in users:
        record = {
            "user_id": user["id"],
            "profile_visibility": random.choice(PRIVACY_LEVELS),
            "show_email": random.choice([True, False]),
            "show_birth_date": random.choice([True, False]),
            "updated_at": datetime.now(timezone.utc).strftime("%Y.%m.%d %H:%M"),
        }
        user_privacy.append(record)
    return user_privacy


# print(gen_user_privacy(gen_user()))


def gen_user_status(users: List[Dict]) -> List[Dict]:
    """Генерация user_status"""
    user_status = []
    for user in users:
        record = {"user_id": user["id"], "status": random.choice(STATUS_OPTIONS)}
        user_status.append(record)
    return user_status


# print(gen_user_status(gen_user()))
