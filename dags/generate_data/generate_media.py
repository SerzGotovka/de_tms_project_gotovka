
import random
from faker import Faker
import uuid
from typing import List, Dict, Any
from generate_data.generate_users import gen_user
import logging

fake = Faker()

users = gen_user()


def generate_photos(users: List[Dict[str, Any]], num_photos: int = 10) -> List[Dict[str, Any]]:
    """Генерация фото"""
    photos = []
    for _ in range(num_photos):
        user = random.choice(users)
        photos.append(
            {
                "id": str(uuid.uuid4()),
                "user_id": user["id"],  
                "filename": fake.file_name(extension="jpg"),
                "url": fake.image_url(width=1920, height=1080),
                "description": fake.sentence(nb_words=6),
                "uploaded_at": fake.date_between(start_date="-1y", end_date="today"),
                "is_private": random.choice([True, False]),
            }
        )
    logging.info(f"Сгенерировано фото: {len(photos)}")
    logging.info(photos)
    return photos


def generate_videos(users: List[Dict[str, Any]], num_videos: int = 5) -> List[Dict[str, Any]]:
    """Генерация видео"""
    videos = []
    for _ in range(num_videos):
        user = random.choice(users)
        videos.append(
            {
                "id": str(uuid.uuid4()),
                "user_id": user["id"],  # Используем id пользователя
                "title": fake.sentence(nb_words=4),
                "url": fake.url() + fake.file_name(extension="mp4"),
                "duration_seconds": random.randint(10, 300),
                "uploaded_at": fake.date_between(start_date="-1y", end_date="today"),
                "visibility": random.choice(["public", "private", "unlisted"]),
            }
        )
    logging.info(f"Сгенерировано видео: {len(videos)}")
    logging.info(videos)
    return videos


def generate_albums(users: List[Dict[str, Any]], photos: List[Dict[str, Any]], videos: List[Dict[str, Any]], num_albums: int = 3) -> List[Dict[str, Any]]:
    """Генерация альбомов"""
    albums = []
    for _ in range(num_albums):
        user = random.choice(users)
        selected_photos = random.sample([p["id"] for p in photos], k=random.randint(1, 5))
        selected_videos = random.sample([v["id"] for v in videos], k=random.randint(0, 2))

        albums.append(
            {
                "id": str(uuid.uuid4()),
                "user_id": user["id"],  # Используем id пользователя
                "title": fake.sentence(nb_words=3),
                "description": fake.paragraph(nb_sentences=2),
                "created_at": fake.date_between(start_date="-1y", end_date="today"),
                "media_ids": selected_photos + selected_videos,  # Список ID медиафайлов
            }
        )
    logging.info(f"Сгенерировано альбомов: {len(albums)}")
    logging.info(albums)
    return albums

def generate_all_media(users):
    # Генерация пользователей
    users = gen_user()

    # Генерация медиа
    photos = generate_photos(users, num_photos=10)
    videos = generate_videos(users, num_videos=5)
    albums = generate_albums(users, photos, videos, num_albums=3)

    return {
        'photos': photos,
        'videos': videos,
        'albums': albums
    }

#