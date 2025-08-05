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



def gen_user() -> List[Dict]:
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
    
    print(f"Сгенерировано пользователей: {len(users)}")
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
    
    print(f"Сгенерировано профилей пользователей: {len(profiles)}")
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
    
    print(f"Сгенерировано настроек пользователей: {len(settings)}")
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
    
    print(f"Сгенерировано настроек приватности: {len(privacies)}")
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
    
    print(f"Сгенерировано статусов пользователей: {len(statuses)}")
    return statuses


if __name__ == "__main__":
    # Генерируем пользователей
    users = gen_user()
    
    # Генерируем профили для пользователей
    profiles = gen_user_profile(users)
    
    # Генерируем настройки для пользователей
    settings = gen_user_settings(users)
    
    # Генерируем настройки приватности для пользователей
    privacies = gen_user_privacy(users)
    
    # Генерируем статусы для пользователей
    statuses = gen_user_status(users)
    
    print("\nИтоговая статистика:")
    print(f"  users: {len(users)} строк")
    print(f"  user_profiles: {len(profiles)} строк")
    print(f"  user_settings: {len(settings)} строк")
    print(f"  user_privacy: {len(privacies)} строк")
    print(f"  user_status: {len(statuses)} строк")
