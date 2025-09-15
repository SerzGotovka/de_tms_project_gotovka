import random
from faker import Faker
from datetime import datetime, timedelta
from typing import List, Dict
from utils.config_generate import REACTION_TYPES
import logging

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
            'id': i + 1,
            'author_id': random.choice(user_ids),
            'caption': fake.sentence(),
            'image_url': fake.image_url(width=640, height=480),
            'location': fake.city(),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано постов: {len(posts)}")
    logging.info(posts)
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
            'id': i + 1,
            'user_id': random.choice(user_ids),
            'image_url': fake.image_url(width=640, height=480),
            'caption': fake.sentence(),
            'expires_at': (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.%f")
        })
    logging.info(f"Сгенерировано историй: {len(stories)}")
    logging.info(stories)
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
            'id': i + 1,
            'user_id': random.choice(user_ids),
            'video_url': 'https://example.com/video.mp4',
            'caption': fake.sentence(),
            'music': fake.sentence(nb_words=2),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано Reels: {len(reels)}")
    logging.info(reels)
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
            'id': i + 1,
            'post_id': random.choice(posts)['id'],
            'user_id': random.choice(user_ids),
            'text': fake.sentence(),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано комментариев: {len(comments)}")
    logging.info(comments)
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
            'id': i + 1,
            'comment_id': comment['id'],
            'user_id': random.choice(user_ids),
            'text': fake.sentence(),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано ответов на комментарии: {len(replies)}")
    logging.info(replies)
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
            'id': i + 1,
            'post_id': post['id'],
            'user_id': random.choice(user_ids),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано лайков: {len(likes)}")
    logging.info(likes)
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
            'id': i + 1,
            'post_id': post['id'],
            'user_id': random.choice(user_ids),
            'type': random.choice(REACTION_TYPES),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано реакций: {len(reactions)}")
    logging.info(reactions)
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
            'id': i + 1,
            'post_id': post['id'],
            'user_id': random.choice(user_ids),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    logging.info(f"Сгенерировано репостов: {len(shares)}")  
    logging.info(shares)
    context["task_instance"].xcom_push(key="num_shares", value=len(shares))
    return shares

