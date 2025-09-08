from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from enum import Enum
from .pydantic.users_pydantic import (
    User,
    UserProfile,
    UserSettings,
    UserPrivacy,
    UserStatusModel,
)
from .pydantic.groups_pydantic import (
    Community,
    Group,
    GroupMember,
    CommunityTopic,
    PinnedPost,
)
from .pydantic.social_pydantic import (
    Friend,
    Follower,
    Subscription,
    Block,
    Mute,
    CloseFriend,
)


# ==================== ENUMS ====================
class ReactionType(str, Enum):
    """Типы реакций"""

    LIKE = "like"
    LOVE = "love"
    HAHA = "haha"
    WOW = "wow"
    SAD = "sad"
    ANGRY = "angry"


class MediaVisibility(str, Enum):
    """Видимость медиа"""

    PUBLIC = "public"
    PRIVATE = "private"
    UNLISTED = "unlisted"


# ==================== CONTENT MODELS ====================


class Post(BaseModel):
    """Модель поста"""

    id: int = Field(..., description="Уникальный идентификатор поста")
    author_id: str = Field(..., description="ID автора поста")
    caption: str = Field(..., description="Подпись к посту")
    image_url: str = Field(..., description="URL изображения")
    location: str = Field(..., description="Местоположение")
    created_at: str = Field(..., description="Дата создания")

    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "author_id": "123e4567-e89b-12d3-a456-426614174000",
                "caption": "Beautiful sunset today!",
                "image_url": "https://example.com/sunset.jpg",
                "location": "New York",
                "created_at": "2024.01.15 18:30",
            }
        }


class Story(BaseModel):
    """Модель истории"""

    id: int = Field(..., description="Уникальный идентификатор истории")
    user_id: str = Field(..., description="ID пользователя")
    image_url: str = Field(..., description="URL изображения")
    caption: str = Field(..., description="Подпись к истории")
    expires_at: str = Field(..., description="Дата истечения")

    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "image_url": "https://example.com/story.jpg",
                "caption": "Quick update!",
                "expires_at": "2024-01-16T18:30:00.000000",
            }
        }


class Reel(BaseModel):
    """Модель Reels"""

    id: int = Field(..., description="Уникальный идентификатор Reels")
    user_id: str = Field(..., description="ID пользователя")
    video_url: str = Field(..., description="URL видео")
    caption: str = Field(..., description="Подпись к Reels")
    music: str = Field(..., description="Музыка")
    created_at: str = Field(..., description="Дата создания")

    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "video_url": "https://example.com/video.mp4",
                "caption": "Dancing to my favorite song!",
                "music": "Popular Song",
                "created_at": "2024.01.15 19:00",
            }
        }


class Comment(BaseModel):
    """Модель комментария"""

    id: int = Field(..., description="Уникальный идентификатор комментария")
    post_id: int = Field(..., description="ID поста")
    user_id: str = Field(..., description="ID пользователя")
    text: str = Field(..., description="Текст комментария")
    created_at: str = Field(..., description="Дата создания")

    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "post_id": 1,
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "text": "Amazing photo!",
                "created_at": "2024.01.15 18:35",
            }
        }


class Reply(BaseModel):
    """Модель ответа на комментарий"""

    id: int = Field(..., description="Уникальный идентификатор ответа")
    comment_id: int = Field(..., description="ID комментария")
    user_id: str = Field(..., description="ID пользователя")
    text: str = Field(..., description="Текст ответа")
    created_at: str = Field(..., description="Дата создания")

    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "comment_id": 1,
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "text": "Thank you!",
                "created_at": "2024.01.15 18:40",
            }
        }


class Like(BaseModel):
    """Модель лайка"""

    id: int = Field(..., description="Уникальный идентификатор лайка")
    post_id: int = Field(..., description="ID поста")
    user_id: str = Field(..., description="ID пользователя")
    created_at: str = Field(..., description="Дата создания")

    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "post_id": 1,
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "created_at": "2024.01.15 18:35",
            }
        }


class Reaction(BaseModel):
    """Модель реакции"""

    id: int = Field(..., description="Уникальный идентификатор реакции")
    post_id: int = Field(..., description="ID поста")
    user_id: str = Field(..., description="ID пользователя")
    type: ReactionType = Field(..., description="Тип реакции")
    created_at: str = Field(..., description="Дата создания")

    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "post_id": 1,
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "type": "love",
                "created_at": "2024.01.15 18:35",
            }
        }


class Share(BaseModel):
    """Модель репоста"""

    id: int = Field(..., description="Уникальный идентификатор репоста")
    post_id: int = Field(..., description="ID поста")
    user_id: str = Field(..., description="ID пользователя")
    created_at: str = Field(..., description="Дата создания")

    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "post_id": 1,
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "created_at": "2024.01.15 18:35",
            }
        }


# ==================== MEDIA MODELS ====================


class Photo(BaseModel):
    """Модель фотографии"""

    id: str = Field(..., description="Уникальный идентификатор фотографии")
    user_id: str = Field(..., description="ID пользователя")
    filename: str = Field(..., description="Имя файла")
    url: str = Field(..., description="URL фотографии")
    description: str = Field(..., description="Описание фотографии")
    uploaded_at: datetime = Field(..., description="Дата загрузки")
    is_private: bool = Field(..., description="Приватная ли фотография")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174016",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "filename": "vacation_photo.jpg",
                "url": "https://example.com/photos/vacation_photo.jpg",
                "description": "Beautiful sunset at the beach",
                "uploaded_at": "2024-01-15T18:30:00",
                "is_private": False,
            }
        }


class Video(BaseModel):
    """Модель видео"""

    id: str = Field(..., description="Уникальный идентификатор видео")
    user_id: str = Field(..., description="ID пользователя")
    title: str = Field(..., description="Название видео")
    url: str = Field(..., description="URL видео")
    duration_seconds: int = Field(..., description="Длительность в секундах")
    uploaded_at: datetime = Field(..., description="Дата загрузки")
    visibility: MediaVisibility = Field(..., description="Видимость видео")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174017",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "title": "My Travel Vlog",
                "url": "https://example.com/videos/travel_vlog.mp4",
                "duration_seconds": 180,
                "uploaded_at": "2024-01-15T19:00:00",
                "visibility": "public",
            }
        }


class Album(BaseModel):
    """Модель альбома"""

    id: str = Field(..., description="Уникальный идентификатор альбома")
    user_id: str = Field(..., description="ID пользователя")
    title: str = Field(..., description="Название альбома")
    description: str = Field(..., description="Описание альбома")
    created_at: datetime = Field(..., description="Дата создания")
    media_ids: List[str] = Field(..., description="Список ID медиафайлов")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174018",
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "title": "Summer Vacation 2024",
                "description": "Photos and videos from my summer vacation",
                "created_at": "2024-01-15T20:00:00",
                "media_ids": [
                    "123e4567-e89b-12d3-a456-426614174016",
                    "123e4567-e89b-12d3-a456-426614174017",
                ],
            }
        }


# ==================== COLLECTION MODELS ====================


class UserCollection(BaseModel):
    """Коллекция пользователей"""

    users: List[User] = Field(..., description="Список пользователей")
    profiles: List[UserProfile] = Field(..., description="Список профилей")
    settings: List[UserSettings] = Field(..., description="Список настроек")
    privacy_settings: List[UserPrivacy] = Field(
        ..., description="Список настроек приватности"
    )
    statuses: List[UserStatusModel] = Field(..., description="Список статусов")


class ContentCollection(BaseModel):
    """Коллекция контента"""

    posts: List[Post] = Field(..., description="Список постов")
    stories: List[Story] = Field(..., description="Список историй")
    reels: List[Reel] = Field(..., description="Список Reels")
    comments: List[Comment] = Field(..., description="Список комментариев")
    replies: List[Reply] = Field(..., description="Список ответов")
    likes: List[Like] = Field(..., description="Список лайков")
    reactions: List[Reaction] = Field(..., description="Список реакций")
    shares: List[Share] = Field(..., description="Список репостов")


class GroupCollection(BaseModel):
    """Коллекция групп"""

    communities: List[Community] = Field(..., description="Список сообществ")
    groups: List[Group] = Field(..., description="Список групп")
    group_members: List[GroupMember] = Field(..., description="Список участников групп")
    community_topics: List[CommunityTopic] = Field(
        ..., description="Список тем сообществ"
    )
    pinned_posts: List[PinnedPost] = Field(
        ..., description="Список закрепленных постов"
    )


class SocialCollection(BaseModel):
    """Коллекция социальных связей"""

    friends: List[Friend] = Field(..., description="Список друзей")
    followers: List[Follower] = Field(..., description="Список подписчиков")
    subscriptions: List[Subscription] = Field(..., description="Список подписок")
    blocks: List[Block] = Field(..., description="Список блокировок")
    mutes: List[Mute] = Field(..., description="Список отключенных уведомлений")
    close_friends: List[CloseFriend] = Field(..., description="Список близких друзей")


class MediaCollection(BaseModel):
    """Коллекция медиа"""

    photos: List[Photo] = Field(..., description="Список фотографий")
    videos: List[Video] = Field(..., description="Список видео")
    albums: List[Album] = Field(..., description="Список альбомов")


class AllDataCollection(BaseModel):
    """Полная коллекция всех данных"""

    users: UserCollection = Field(..., description="Данные пользователей")
    content: ContentCollection = Field(..., description="Контент")
    groups: GroupCollection = Field(..., description="Группы")
    social: SocialCollection = Field(..., description="Социальные связи")
    media: MediaCollection = Field(..., description="Медиа")
