from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum


# ==================== ENUMS ====================
class FriendStatus(str, Enum):
    """Статусы дружбы"""

    ACTIVE = "active"
    PENDING = "pending"
    BLOCKED = "blocked"


class BlockReason(str, Enum):
    """Причины блокировки"""

    SPAM = "spam"
    HARASSMENT = "harassment"
    UNFOLLOW = "unfollow"
    OTHER = "other"


# ==================== SOCIAL MODELS ====================


class Friend(BaseModel):
    """Модель дружбы"""

    id: str = Field(..., description="Уникальный идентификатор дружбы")
    user_id: str = Field(..., description="ID пользователя")
    friend_id: str = Field(..., description="ID друга")
    created_at: str = Field(..., description="Дата создания")
    status: FriendStatus = Field(..., description="Статус дружбы")
    is_best_friend: bool = Field(..., description="Лучший ли друг")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174010",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "friend_id": "123e4567-e89b-12d3-a456-426614174001",
                "created_at": "2023.05.15 10:30",
                "status": "active",
                "is_best_friend": True,
            }
        }


class Follower(BaseModel):
    """Модель подписчика"""

    id: str = Field(..., description="Уникальный идентификатор подписки")
    user_id: str = Field(..., description="ID пользователя")
    follower_id: str = Field(..., description="ID подписчика")
    followed_at: str = Field(..., description="Дата подписки")
    is_active: bool = Field(..., description="Активна ли подписка")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174011",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "follower_id": "123e4567-e89b-12d3-a456-426614174001",
                "followed_at": "2023.04.15 14:20",
                "is_active": True,
            }
        }


class Subscription(BaseModel):
    """Модель подписки"""

    id: str = Field(..., description="Уникальный идентификатор подписки")
    user_id: str = Field(..., description="ID пользователя")
    subscribed_to: str = Field(..., description="ID пользователя на которого подписан")
    subscribed_at: str = Field(..., description="Дата подписки")
    is_active: bool = Field(..., description="Активна ли подписка")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174012",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "subscribed_to": "123e4567-e89b-12d3-a456-426614174001",
                "subscribed_at": "2023.03.15 16:45",
                "is_active": True,
            }
        }


class Block(BaseModel):
    """Модель блокировки"""

    id: str = Field(..., description="Уникальный идентификатор блокировки")
    user_id: str = Field(..., description="ID пользователя")
    blocked_id: str = Field(..., description="ID заблокированного пользователя")
    blocked_at: str = Field(..., description="Дата блокировки")
    reason: BlockReason = Field(..., description="Причина блокировки")
    is_active: bool = Field(..., description="Активна ли блокировка")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174013",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "blocked_id": "123e4567-e89b-12d3-a456-426614174001",
                "blocked_at": "2023.02.15 18:30",
                "reason": "spam",
                "is_active": True,
            }
        }


class Mute(BaseModel):
    """Модель отключения уведомлений"""

    id: str = Field(..., description="Уникальный идентификатор отключения")
    user_id: str = Field(..., description="ID пользователя")
    muted_id: str = Field(
        ..., description="ID пользователя от которого отключены уведомления"
    )
    muted_at: str = Field(..., description="Дата отключения")
    duration_days: Optional[int] = Field(
        None, description="Длительность в днях (None = бессрочно)"
    )
    is_active: bool = Field(..., description="Активно ли отключение")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174014",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "muted_id": "123e4567-e89b-12d3-a456-426614174001",
                "muted_at": "2023.01.15 20:00",
                "duration_days": 30,
                "is_active": True,
            }
        }


class CloseFriend(BaseModel):
    """Модель близкого друга"""

    id: str = Field(..., description="Уникальный идентификатор близкого друга")
    user_id: str = Field(..., description="ID пользователя")
    close_friend_id: str = Field(..., description="ID близкого друга")
    added_at: str = Field(..., description="Дата добавления")
    is_active: bool = Field(..., description="Активен ли статус")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174015",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "close_friend_id": "123e4567-e89b-12d3-a456-426614174001",
                "added_at": "2023.05.15 10:30",
                "is_active": True,
            }
        }
