import random
from faker import Faker 
import uuid
from typing import List, Dict, Any
import logging

# from utils.function_kafka import PHOTOS_TOPIC, send_to_kafka

fake = Faker()


def generate_photos(num_photos: int = 10, users: List[Dict[str, Any]] = None, **context) -> List[Dict[str, Any]]:
    """Генерация фото"""
    if users is None:
        users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    
    if not users:
        raise ValueError("Список пользователей пуст или None")
    
    photos = []
    
    for _ in range(num_photos):
        user = random.choice(users)
        photo = {
            "id": str(uuid.uuid4()),
            "user_id": user["id"],  
            "filename": fake.file_name(extension="jpg"),
            "url": fake.image_url(width=1920, height=1080),
            "description": fake.sentence(nb_words=6),
            "uploaded_at": fake.date_between(start_date="-1y", end_date="today").isoformat(),
            "is_private": random.choice([True, False]),
        }
        photos.append(photo)
        
        # Исправлено: отправляем каждое фото в Kafka
        # send_to_kafka(PHOTOS_TOPIC, photo['id'], str(photo))

    logging.info(f"Сгенерировано фото: {len(photos)}")

    context["task_instance"].xcom_push(key="photos", value=photos)
    context["task_instance"].xcom_push(key="number_photos", value=len(photos))

    return photos


def generate_videos(num_videos: int = 5, users: List[Dict[str, Any]] = None, **context) -> List[Dict[str, Any]]:
    """Генерация видео"""
    if users is None:
        users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    
    if not users:
        raise ValueError("Список пользователей пуст или None")
    
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
                "uploaded_at": fake.date_between(start_date="-1y", end_date="today").isoformat(),
                "visibility": random.choice(["public", "private", "unlisted"]),
            }
        )
    number_videos = len(videos)
    logging.info(f"Сгенерировано видео: {len(videos)}")

    context["task_instance"].xcom_push(key="videos", value=videos)
    context["task_instance"].xcom_push(key="number_videos", value=number_videos)
    return videos


def generate_albums(num_albums: int = 3, users: List[Dict[str, Any]] = None, photos: List[Dict[str, Any]] = None, videos: List[Dict[str, Any]] = None, **context) -> List[Dict[str, Any]]:
    """Генерация альбомов"""
    if users is None:
        users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if photos is None:
        photos = context["task_instance"].xcom_pull(key="photos", task_ids="generate_data_group.gen_photos")
    if videos is None:
        videos = context["task_instance"].xcom_pull(key="videos", task_ids="generate_data_group.gen_videos")
    
    if not users:
        raise ValueError("Список пользователей пуст или None")
    if not photos:
        raise ValueError("Список фотографий пуст или None")
    if not videos:
        raise ValueError("Список видео пуст или None")
    
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
                "created_at": fake.date_between(start_date="-1y", end_date="today").isoformat(),
                "media_ids": selected_photos + selected_videos,  # Список ID медиафайлов
            }
        )
    number_albums = len(albums)
    logging.info(f"Сгенерировано альбомов: {len(albums)}")

    context["task_instance"].xcom_push(key="albums", value=albums)
    context["task_instance"].xcom_push(key="number_albums", value=number_albums)
    return albums

# def generate_all_media(users):
#     # Генерация медиа
#     photos = generate_photos(users, num_photos=10)
#     videos = generate_videos(users, num_videos=5)
#     albums = generate_albums(users, photos, videos, num_albums=3)

#     return {
#         'photos': photos,
#         'videos': videos,
#         'albums': albums
#     }

#