from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum
from uuid import UUID


# ==================== ENUMS ====================
class GroupPrivacy(str, Enum):
    """Приватность групп"""

    PUBLIC = "public"
    PRIVATE = "private"
    HIDDEN = "hidden"


class GroupRole(str, Enum):
    """Роли в группах"""

    MEMBER = "member"
    MODERATOR = "moderator"
    ADMIN = "admin"


# ==================== GROUP MODELS ====================


class Community(BaseModel):
    """Модель сообщества"""

    id: UUID = Field(..., description="Уникальный идентификатор сообщества")
    name: str = Field(..., description="Название сообщества")
    description: str = Field(..., description="Описание сообщества")
    created_at: str = Field(..., description="Дата создания")
    member_count: int = Field(..., description="Количество участников")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174005",
                "name": "Tech Enthusiasts",
                "description": "A community for technology lovers",
                "created_at": "2023.06.15 10:00",
                "member_count": 1500,
            }
        }


class Group(BaseModel):
    """Модель группы"""

    id: UUID = Field(..., description="Уникальный идентификатор группы")
    owner_id: UUID = Field(..., description="ID владельца группы")
    name: str = Field(..., description="Название группы")
    description: str = Field(..., description="Описание группы")
    privacy: GroupPrivacy = Field(..., description="Приватность группы")
    created_at: str = Field(..., description="Дата создания")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174006",
                "owner_id": "123e4567-e89b-12d3-a456-426614174000",
                "name": "Local Photography Club",
                "description": "A group for local photographers to share their work",
                "privacy": "public",
                "created_at": "2023.08.15 14:30",
            }
        }


class GroupMember(BaseModel):
    """Модель участника группы"""

    id: UUID = Field(..., description="Уникальный идентификатор участника")
    group_id: UUID = Field(..., description="ID группы")
    user_id: UUID = Field(..., description="ID пользователя")
    joined_at: str = Field(..., description="Дата присоединения")
    role: GroupRole = Field(..., description="Роль в группе")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174007",
                "group_id": "123e4567-e89b-12d3-a456-426614174006",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "joined_at": "2023.08.20 16:00",
                "role": "member",
            }
        }


class CommunityTopic(BaseModel):
    """Модель темы сообщества"""

    id: UUID = Field(..., description="Уникальный идентификатор темы")
    community_id: UUID = Field(..., description="ID сообщества")
    title: str = Field(..., description="Заголовок темы")
    description: str = Field(..., description="Описание темы")
    created_at: str = Field(..., description="Дата создания")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174008",
                "community_id": "123e4567-e89b-12d3-a456-426614174005",
                "title": "Latest AI Developments",
                "description": "Discussion about recent advances in artificial intelligence",
                "created_at": "2023.07.15 12:00",
            }
        }


class PinnedPost(BaseModel):
    """Модель закрепленного поста"""

    id: UUID = Field(..., description="Уникальный идентификатор закрепленного поста")
    community_id: Optional[UUID] = Field(None, description="ID сообщества")
    group_id: Optional[UUID] = Field(None, description="ID группы")
    author_id: UUID = Field(..., description="ID автора")
    content: str = Field(..., description="Содержимое поста")
    pinned_at: str = Field(..., description="Дата закрепления")
    expires_at: str = Field(..., description="Дата истечения")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174009",
                "community_id": "123e4567-e89b-12d3-a456-426614174005",
                "group_id": None,
                "author_id": "123e4567-e89b-12d3-a456-426614174000",
                "content": "Welcome to our community! Please read the rules.",
                "pinned_at": "2023.06.15 10:00",
                "expires_at": "2024.06.15 10:00",
            }
        }
