from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from enum import Enum


# ==================== ENUMS ====================
class Gender(str, Enum):
    """Пол пользователя"""

    MALE = "male"
    FEMALE = "female"
    OTHER = "other"
    PREFER_NOT_TO_SAY = "prefer_not_to_say"


class Language(str, Enum):
    """Языки"""

    EN = "en"
    RU = "ru"
    ES = "es"
    FR = "fr"
    DE = "de"


class Timezone(str, Enum):
    """Часовые пояса"""

    UTC = "UTC"
    UTC_PLUS_1 = "UTC+1"
    UTC_PLUS_2 = "UTC+2"
    UTC_PLUS_3 = "UTC+3"
    UTC_MINUS_5 = "UTC-5"
    UTC_MINUS_8 = "UTC-8"


class Theme(str, Enum):
    """Темы интерфейса"""

    LIGHT = "light"
    DARK = "dark"
    AUTO = "auto"


class PrivacyLevel(str, Enum):
    """Уровни приватности"""

    PUBLIC = "public"
    FRIENDS = "friends"
    PRIVATE = "private"


class UserStatus(str, Enum):
    """Статусы пользователей"""

    ONLINE = "online"
    OFFLINE = "offline"


# ==================== USER MODELS ====================
class User(BaseModel):
    """Модель пользователя"""

    id: str = Field(..., description="Уникальный идентификатор пользователя")
    username: str = Field(..., description="Имя пользователя")
    email: str = Field(..., description="Email адрес")
    password_hash: str = Field(..., description="Хеш пароля")
    created_at: str = Field(..., description="Дата создания аккаунта")
    last_login: str = Field(..., description="Дата последнего входа")
    is_active: bool = Field(..., description="Активен ли аккаунт")
    is_verified: bool = Field(..., description="Верифицирован ли аккаунт")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "username": "john_doe",
                "email": "john@example.com",
                "password_hash": "a1b2c3d4e5f6...",
                "created_at": "2023.01.15 10:30",
                "last_login": "2024.01.15 14:20",
                "is_active": True,
                "is_verified": False,
            }
        }


class UserProfile(BaseModel):
    """Модель профиля пользователя"""

    id: str = Field(..., description="Уникальный идентификатор профиля")
    user_id: str = Field(..., description="ID пользователя")
    first_name: str = Field(..., description="Имя")
    last_name: str = Field(..., description="Фамилия")
    bio: str = Field(..., description="Биография")
    avatar_url: str = Field(..., description="URL аватара")
    cover_photo_url: str = Field(..., description="URL обложки")
    location: str = Field(..., description="Местоположение")
    website: str = Field(..., description="Веб-сайт")
    phone: str = Field(..., description="Телефон")
    birth_date: str = Field(..., description="Дата рождения")
    gender: Gender = Field(..., description="Пол")
    badges: List[str] = Field(default_factory=list, description="Список бейджей")
    updated_at: str = Field(..., description="Дата последнего обновления")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174001",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "first_name": "John",
                "last_name": "Doe",
                "bio": "Software developer and tech enthusiast",
                "avatar_url": "https://example.com/avatar.jpg",
                "cover_photo_url": "https://example.com/cover.jpg",
                "location": "New York",
                "website": "https://johndoe.com",
                "phone": "+1234567890",
                "birth_date": "1990.05.15",
                "gender": "male",
                "badges": ["verified", "top-contributor"],
                "updated_at": "2024.01.15 16:45",
            }
        }


class UserSettings(BaseModel):
    """Модель настроек пользователя"""

    id: str = Field(..., description="Уникальный идентификатор настроек")
    user_id: str = Field(..., description="ID пользователя")
    language: Language = Field(..., description="Язык интерфейса")
    timezone: Timezone = Field(..., description="Часовой пояс")
    theme: Theme = Field(..., description="Тема интерфейса")
    notifications_enabled: bool = Field(..., description="Включены ли уведомления")
    email_notifications: bool = Field(..., description="Email уведомления")
    push_notifications: bool = Field(..., description="Push уведомления")
    two_factor_enabled: bool = Field(..., description="Двухфакторная аутентификация")
    auto_save_drafts: bool = Field(..., description="Автосохранение черновиков")
    created_at: str = Field(..., description="Дата создания")
    updated_at: str = Field(..., description="Дата обновления")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174002",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "language": "en",
                "timezone": "UTC",
                "theme": "dark",
                "notifications_enabled": True,
                "email_notifications": True,
                "push_notifications": False,
                "two_factor_enabled": True,
                "auto_save_drafts": True,
                "created_at": "2023.01.15 10:30",
                "updated_at": "2024.01.15 14:20",
            }
        }


class UserPrivacy(BaseModel):
    """Модель настроек приватности пользователя"""

    id: str = Field(..., description="Уникальный идентификатор настроек приватности")
    user_id: str = Field(..., description="ID пользователя")
    profile_visibility: PrivacyLevel = Field(..., description="Видимость профиля")
    posts_visibility: PrivacyLevel = Field(..., description="Видимость постов")
    friends_visibility: PrivacyLevel = Field(..., description="Видимость друзей")
    photos_visibility: PrivacyLevel = Field(..., description="Видимость фотографий")
    contact_info_visibility: PrivacyLevel = Field(
        ..., description="Видимость контактной информации"
    )
    search_visibility: bool = Field(..., description="Видимость в поиске")
    allow_friend_requests: bool = Field(..., description="Разрешить запросы в друзья")
    allow_messages: bool = Field(..., description="Разрешить сообщения")
    show_online_status: bool = Field(..., description="Показывать статус онлайн")
    created_at: str = Field(..., description="Дата создания")
    updated_at: str = Field(..., description="Дата обновления")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174003",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "profile_visibility": "public",
                "posts_visibility": "friends",
                "friends_visibility": "private",
                "photos_visibility": "friends",
                "contact_info_visibility": "private",
                "search_visibility": True,
                "allow_friend_requests": True,
                "allow_messages": True,
                "show_online_status": False,
                "created_at": "2023.01.15 10:30",
                "updated_at": "2024.01.15 14:20",
            }
        }


class UserStatusModel(BaseModel):
    """Модель статуса пользователя"""

    id: str = Field(..., description="Уникальный идентификатор статуса")
    user_id: str = Field(..., description="ID пользователя")
    status: UserStatus = Field(..., description="Статус пользователя")
    status_message: Optional[str] = Field(None, description="Сообщение статуса")
    last_seen: str = Field(..., description="Дата последнего посещения")
    is_online: bool = Field(..., description="Онлайн ли пользователь")
    created_at: str = Field(..., description="Дата создания")
    updated_at: str = Field(..., description="Дата обновления")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174004",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "online",
                "status_message": "Working on a new project",
                "last_seen": "2024.01.15 16:45",
                "is_online": True,
                "created_at": "2023.01.15 10:30",
                "updated_at": "2024.01.15 16:45",
            }
        }
