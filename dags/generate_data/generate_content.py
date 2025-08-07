import random

from faker import Faker
from datetime import datetime, timedelta
from typing import List, Dict
from generate_data.generate_users import gen_user
from utils.config_generate import REACTION_TYPES
import logging

fake = Faker()




def generate_posts(user_ids: List[str], n=5) -> List[Dict]:
    """Генерация постов"""
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
    return posts


def generate_stories(user_ids: List[str], n=5) -> List[Dict]:
    """Генерация историй"""
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
    return stories


def generate_reels(user_ids: List[str], n=5) -> List[Dict]:
    """Генерация Reels"""
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
    return reels


def generate_comments(user_ids: List[str], posts: List[Dict], n=10) -> List[Dict]:
    """Генерация комментариев"""
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
    return comments


def generate_replies(user_ids: List[str], comments: List[Dict], n=10) -> List[Dict]:
    """Генерация ответов на комментарии"""
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
    return replies


def generate_likes(user_ids: List[str], posts: List[Dict], n=20) -> List[Dict]:
    """Генерация лайков"""
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
    return likes


def generate_reactions(user_ids: List[str], posts: List[Dict], n=15) -> List[Dict]:
    """Генерация реакций"""
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
    return reactions


def generate_shares(user_ids: List[str], posts: List[Dict], n=10) -> List[Dict]:
    """Генерация репостов"""
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
    return shares


def generate_all_data(users: List[Dict]) -> Dict[str, List[Dict]]:
    """Финальная генерация всех данных"""
    user_ids = [user['id'] for user in users]

    
    posts = generate_posts(user_ids)
    stories = generate_stories(user_ids)
    reels = generate_reels(user_ids)
    comments = generate_comments(user_ids, posts)
    replies = generate_replies(user_ids, comments)
    likes = generate_likes(user_ids, posts)
    reactions = generate_reactions(user_ids, posts)
    shares = generate_shares(user_ids, posts)

    # 3. Возвращаем только нужные структуры (без пользователей!)
    return {
        'posts': posts,
        'stories': stories,
        'reels': reels,
        'comments': comments,
        'replies': replies,
        'likes': likes,
        'reactions': reactions,
        'shares': shares
    }

# # Пример использования
# if __name__ == "__main__":
#     users = gen_user()
#     data = generate_all_data(users)  # <-- В data НЕТ информации о пользователях

#     # Вывод первых 2 записей каждого типа
#     for key, value in data.items():
#         print(f"\n{key.upper()}:")
#         for item in value:
#             print(item)