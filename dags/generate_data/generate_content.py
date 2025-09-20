import random
from faker import Faker
from datetime import datetime, timedelta
from typing import List, Dict
from utils.config_generate import REACTION_TYPES
import logging
import uuid

from utils.function_minio import save_data_directly_to_minio

fake = Faker()




def generate_posts(n=5, **context) -> List[Dict]:
    """Генерация постов"""
    # Пытаемся получить пользователей из XCom, если не получается - генерируем самостоятельно
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        # Если нет данных в XCom, генерируем пользователей самостоятельно
        from generate_data.generate_users import gen_user
        users = gen_user(n=50, **context)
    
    user_ids = [u["id"] for u in users]
    
    posts = []
    for i in range(n):
        posts.append({
            'id': str(uuid.uuid4()),
            'author_id': random.choice(user_ids),
            'caption': fake.sentence(),
            'image_url': fake.image_url(width=640, height=480),
            'location': fake.city(),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано постов: {len(posts)}")
    logging.info(posts)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_posts_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=posts,
            filename=filename,
            folder="posts/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} постов в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи постов в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="posts", value=posts)
    context["task_instance"].xcom_push(key="num_posts", value=len(posts))
    return posts


def generate_stories(n=5, **context) -> List[Dict]:
    """Генерация историй"""
    # Пытаемся получить пользователей из XCom, если не получается - генерируем самостоятельно
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        # Если нет данных в XCom, генерируем пользователей самостоятельно
        from generate_data.generate_users import gen_user
        users = gen_user(n=50, **context)
    
    user_ids = [u["id"] for u in users]
    
    stories = []
    for i in range(n):
        stories.append({
            'id': str(uuid.uuid4()),
            'user_id': random.choice(user_ids),
            'image_url': fake.image_url(width=640, height=480),
            'caption': fake.sentence(),
            'expires_at': (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.%f")
        })
    logging.info(f"Сгенерировано историй: {len(stories)}")
    logging.info(stories)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_stories_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=stories,
            filename=filename,
            folder="stories/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} историй в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи историй в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="stories", value=stories)
    context["task_instance"].xcom_push(key="num_stories", value=len(stories))
    return stories


def generate_reels(n=5, **context) -> List[Dict]:
    """Генерация Reels"""
    # Пытаемся получить пользователей из XCom, если не получается - генерируем самостоятельно
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        # Если нет данных в XCom, генерируем пользователей самостоятельно
        from generate_data.generate_users import gen_user
        users = gen_user(n=50, **context)
    
    user_ids = [u["id"] for u in users]
    
    reels = []
    for i in range(n):
        reels.append({
            'id': str(uuid.uuid4()),
            'user_id': random.choice(user_ids),
            'video_url': 'https://example.com/video.mp4',
            'caption': fake.sentence(),
            'music': fake.sentence(nb_words=2),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано Reels: {len(reels)}")
    logging.info(reels)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_reels_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=reels,
            filename=filename,
            folder="reels/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} Reels в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи Reels в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="reels", value=reels)
    context["task_instance"].xcom_push(key="num_reels", value=len(reels))
    return reels


def generate_comments(n=10, **context) -> List[Dict]:
    """Генерация комментариев"""
    # Пытаемся получить пользователей из XCom, если не получается - генерируем самостоятельно
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        # Если нет данных в XCom, генерируем пользователей самостоятельно
        from generate_data.generate_users import gen_user
        users = gen_user(n=50, **context)
    
    user_ids = [u["id"] for u in users]
    
    # Пытаемся получить посты из XCom, если не получается - генерируем самостоятельно
    posts = context["task_instance"].xcom_pull(key="posts", task_ids="generate_data_group.gen_posts")
    if not posts:
        # Если нет данных в XCom, генерируем посты самостоятельно
        posts = generate_posts(n=10, **context)
    
    comments = []
    for i in range(n):
        comments.append({
            'id': str(uuid.uuid4()),
            'post_id': random.choice(posts)['id'],
            'user_id': random.choice(user_ids),
            'text': fake.sentence(),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано комментариев: {len(comments)}")
    logging.info(comments)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_comments_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=comments,
            filename=filename,
            folder="comments/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} комментариев в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи комментариев в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="comments", value=comments)
    context["task_instance"].xcom_push(key="num_comments", value=len(comments))
    return comments


def generate_replies(n=10, **context) -> List[Dict]:
    """Генерация ответов на комментарии"""
    # Пытаемся получить пользователей из XCom, если не получается - генерируем самостоятельно
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        # Если нет данных в XCom, генерируем пользователей самостоятельно
        from generate_data.generate_users import gen_user
        users = gen_user(n=50, **context)
    
    user_ids = [u["id"] for u in users]
    
    # Пытаемся получить комментарии из XCom, если не получается - генерируем самостоятельно
    comments = context["task_instance"].xcom_pull(key="comments", task_ids="generate_data_group.gen_comments")
    if not comments:
        # Если нет данных в XCom, генерируем комментарии самостоятельно
        comments = generate_comments(n=15, **context)
    
    replies = []
    for i in range(n):
        comment = random.choice(comments)
        replies.append({
            'id': str(uuid.uuid4()),
            'comment_id': comment['id'],
            'user_id': random.choice(user_ids),
            'text': fake.sentence(),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано ответов на комментарии: {len(replies)}")
    logging.info(replies)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_replies_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=replies,
            filename=filename,
            folder="replies/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} ответов в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи ответов в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="replies", value=replies)
    context["task_instance"].xcom_push(key="num_replies", value=len(replies))
    return replies


def generate_likes(n=20, **context) -> List[Dict]:
    """Генерация лайков"""
    # Пытаемся получить пользователей из XCom, если не получается - генерируем самостоятельно
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        # Если нет данных в XCom, генерируем пользователей самостоятельно
        from generate_data.generate_users import gen_user
        users = gen_user(n=50, **context)
    
    user_ids = [u["id"] for u in users]
    
    # Пытаемся получить посты из XCom, если не получается - генерируем самостоятельно
    posts = context["task_instance"].xcom_pull(key="posts", task_ids="generate_data_group.gen_posts")
    if not posts:
        # Если нет данных в XCom, генерируем посты самостоятельно
        posts = generate_posts(n=10, **context)
    
    likes = []
    for i in range(n):
        post = random.choice(posts)
        likes.append({
            'id': str(uuid.uuid4()),
            'post_id': post['id'],
            'user_id': random.choice(user_ids),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано лайков: {len(likes)}")
    logging.info(likes)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_likes_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=likes,
            filename=filename,
            folder="likes/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} лайков в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи лайков в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="likes", value=likes)
    context["task_instance"].xcom_push(key="num_likes", value=len(likes))
    return likes


def generate_reactions(n=15, **context) -> List[Dict]:
    """Генерация реакций"""
    # Пытаемся получить пользователей из XCom, если не получается - генерируем самостоятельно
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        # Если нет данных в XCom, генерируем пользователей самостоятельно
        from generate_data.generate_users import gen_user
        users = gen_user(n=50, **context)
    
    user_ids = [u["id"] for u in users]
    
    # Пытаемся получить посты из XCom, если не получается - генерируем самостоятельно
    posts = context["task_instance"].xcom_pull(key="posts", task_ids="generate_data_group.gen_posts")
    if not posts:
        # Если нет данных в XCom, генерируем посты самостоятельно
        posts = generate_posts(n=10, **context)
    
    reactions = []
    for i in range(n):
        post = random.choice(posts)
        reactions.append({
            'id': str(uuid.uuid4()),
            'post_id': post['id'],
            'user_id': random.choice(user_ids),
            'type': random.choice(REACTION_TYPES),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано реакций: {len(reactions)}")
    logging.info(reactions)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_reactions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=reactions,
            filename=filename,
            folder="reactions/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} реакций в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи реакций в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="reactions", value=reactions)
    context["task_instance"].xcom_push(key="num_reactions", value=len(reactions))
    return reactions


def generate_shares(n=10, **context) -> List[Dict]:
    """Генерация репостов"""
    # Пытаемся получить пользователей из XCom, если не получается - генерируем самостоятельно
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        # Если нет данных в XCom, генерируем пользователей самостоятельно
        from generate_data.generate_users import gen_user
        users = gen_user(n=50, **context)
    
    user_ids = [u["id"] for u in users]
    
    # Пытаемся получить посты из XCom, если не получается - генерируем самостоятельно
    posts = context["task_instance"].xcom_pull(key="posts", task_ids="generate_data_group.gen_posts")
    if not posts:
        # Если нет данных в XCom, генерируем посты самостоятельно
        posts = generate_posts(n=10, **context)
    
    shares = []
    for i in range(n):
        post = random.choice(posts)
        shares.append({
            'id': str(uuid.uuid4()),
            'post_id': post['id'],
            'user_id': random.choice(user_ids),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано репостов: {len(shares)}")  
    logging.info(shares)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_shares_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=shares,
            filename=filename,
            folder="shares/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} репостов в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи репостов в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="shares", value=shares)
    context["task_instance"].xcom_push(key="num_shares", value=len(shares))
    return shares

