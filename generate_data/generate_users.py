import uuid
import random
from datetime import datetime, timedelta, timezone
from faker import Faker
from typing import List, Dict

fake = Faker()

# Конфигурация начальных данных
NUM_USERS = 1000  # сколько пользователей сгенерировать

BADGE_NAMES = [
    "verified",
    "top-contributor",
    "beta-tester",
    "early-adopter",
    "vip",
    "moderator",
    "donator",
]
PRIVACY_LEVELS = ["public", "friends", "private"]
STATUS_OPTIONS = ["online", "offline"]


def gen_user() -> Dict:
    """создание структуры user"""
    return {
        "id": str(uuid.uuid4()),
        "username": fake.unique.user_name(),
        "email": fake.unique.email(),
        "password_hash": fake.sha256(),  # хэширование пароля
        "created_at": fake.date_time_between(start_date="-2y", end_date="now"),
        "updated_at": datetime.now(timezone.utc),
    }


def gen_user_profile(user: Dict) -> Dict:
    """создание структуры user_profiles"""
    return {
        "user_id": user["id"],
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "bio": fake.text(max_nb_chars=200),
        "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70),
        "location": fake.city(),
        "updated_at": datetime.now(timezone.utc),
    }


def gen_user_settings(user: Dict) -> Dict:
    """создание структуры user_settings: предпочтения пользователя"""
    return {
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


def gen_user_privacy(user: Dict) -> Dict:
    """создание структуры user_privacy: настройки приватности"""
    return {
        "user_id": user["id"],
        "profile_visibility": random.choice(PRIVACY_LEVELS),
        "show_email": random.choice([True, False]),
        "show_birth_date": random.choice([True, False]),
        "updated_at": datetime.now(timezone.utc),
    }


def gen_user_status(user: Dict) -> Dict:
    """создание структуры user_status: online / offline"""
    return {
        "user_id": user["id"],
        "status": random.choice(STATUS_OPTIONS),
        "last_seen_at": fake.date_time_between(start_date="-3h", end_date="now"),
    }


def gen_user_badges(user: Dict) -> List[Dict]:
    """создание структуры user_badges: пользовательские значки (0-N шт.)"""
    badges = []
    for badge in random.sample(BADGE_NAMES, random.randint(0, 3)):    #используется для случайного выбора       элементов из последовательности без повторений.
        badges.append(
            {
                "user_id": user["id"],
                "badge": badge,
                "awarded_at": fake.date_time_between(
                    start_date=user["created_at"], end_date="now"
                ),
            }
        )
    return badges


def generate_all() -> Dict[str, List]:
    """Генерирует данные для всех таблиц"""
    users = []
    profiles = []
    settings = []
    privacies = []
    statuses = []
    badges = []

    for _ in range(NUM_USERS):
        user = gen_user()
        users.append(user)

        profiles.append(gen_user_profile(user))
        settings.append(gen_user_settings(user))
        privacies.append(gen_user_privacy(user))
        statuses.append(gen_user_status(user))

        badges.extend(gen_user_badges(user))

    return {
        "users": users,
        "user_profiles": profiles,
        "user_settings": settings,
        "user_privacy": privacies,
        "user_status": statuses,
        "user_badges": badges
    }


if __name__ == "__main__":
    data = generate_all()

    print("Сгенерировано:")
    for table, rows in data.items():
        print(f"  {table}: {len(rows)} строк")