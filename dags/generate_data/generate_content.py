import random
from faker import Faker
from datetime import datetime, timedelta
from typing import List, Dict
from utils.config_generate import REACTION_TYPES
import logging

fake = Faker()




def generate_posts(n=5, **context) -> List[Dict]:
    """Генерация постов"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        raise ValueError("Не удалось получить данные пользователей из XCom")
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
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        raise ValueError("Не удалось получить данные пользователей из XCom")
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
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        raise ValueError("Не удалось получить данные пользователей из XCom")
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

    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        raise ValueError("Не удалось получить данные пользователей из XCom")
    user_ids = [u["id"] for u in users]
    
    posts = context["task_instance"].xcom_pull(key="posts", task_ids="generate_data_group.gen_posts")
    if not posts:
        raise ValueError("Не удалось получить данные постов из XCom")
    
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
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        raise ValueError("Не удалось получить данные пользователей из XCom")
    user_ids = [u["id"] for u in users]
    
    comments = context["task_instance"].xcom_pull(key="comments", task_ids="generate_data_group.gen_comments")
    if not comments:
        raise ValueError("Не удалось получить данные комментариев из XCom")
    
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
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        raise ValueError("Не удалось получить данные пользователей из XCom")
    user_ids = [u["id"] for u in users]
    
    posts = context["task_instance"].xcom_pull(key="posts", task_ids="generate_data_group.gen_posts")
    if not posts:
        raise ValueError("Не удалось получить данные постов из XCom")
    
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
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        raise ValueError("Не удалось получить данные пользователей из XCom")
    user_ids = [u["id"] for u in users]
    
    posts = context["task_instance"].xcom_pull(key="posts", task_ids="generate_data_group.gen_posts")
    if not posts:
        raise ValueError("Не удалось получить данные постов из XCom")
    
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
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    if not users:
        raise ValueError("Не удалось получить данные пользователей из XCom")
    user_ids = [u["id"] for u in users]
    
    posts = context["task_instance"].xcom_pull(key="posts", task_ids="generate_data_group.gen_posts")
    if not posts:
        raise ValueError("Не удалось получить данные постов из XCom")
    
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


# def generate_all_data(users: List[Dict]) -> Dict[str, List[Dict]]:
#     """Финальная генерация всех данных"""
#     user_ids = [user['id'] for user in users]

    
#     posts = generate_posts(user_ids)
#     stories = generate_stories(user_ids)
#     reels = generate_reels(user_ids)
#     comments = generate_comments(user_ids, posts)
#     replies = generate_replies(user_ids, comments)
#     likes = generate_likes(user_ids, posts)
#     reactions = generate_reactions(user_ids, posts)
#     shares = generate_shares(user_ids, posts)

#     # 3. Возвращаем только нужные структуры (без пользователей!)
#     return {
#         'posts': posts,
#         'stories': stories,
#         'reels': reels,
#         'comments': comments,
#         'replies': replies,
#         'likes': likes,
#         'reactions': reactions,
#         'shares': shares
#     }
