import random
from faker import Faker
from datetime import datetime, timedelta
from typing import List, Dict
from generate_users import gen_user

fake = Faker()

# Типы реакций
REACTION_TYPES = ['like', 'love', 'haha', 'wow', 'sad', 'angry']

# Генерация постов
def generate_posts(user_ids: List[str], n=5) -> List[Dict]:
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
    return posts

# Генерация историй
def generate_stories(user_ids: List[str], n=5) -> List[Dict]:
    stories = []
    for i in range(n):
        stories.append({
            'id': i + 1,
            'user_id': random.choice(user_ids),
            'image_url': fake.image_url(width=640, height=480),
            'caption': fake.sentence(),
            'expires_at': (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.%f")
        })
    return stories

# Генерация Reels
def generate_reels(user_ids: List[str], n=5) -> List[Dict]:
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
    return reels

# Генерация комментариев
def generate_comments(user_ids: List[str], posts: List[Dict], n=10) -> List[Dict]:
    comments = []
    for i in range(n):
        comments.append({
            'id': i + 1,
            'post_id': random.choice(posts)['id'],
            'user_id': random.choice(user_ids),
            'text': fake.sentence(),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    return comments

# Генерация ответов на комментарии
def generate_replies(user_ids: List[str], comments: List[Dict], n=10) -> List[Dict]:
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
    return replies

# Генерация лайков
def generate_likes(user_ids: List[str], posts: List[Dict], n=20) -> List[Dict]:
    likes = []
    for i in range(n):
        post = random.choice(posts)
        likes.append({
            'id': i + 1,
            'post_id': post['id'],
            'user_id': random.choice(user_ids),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    return likes

# Генерация реакций
def generate_reactions(user_ids: List[str], posts: List[Dict], n=15) -> List[Dict]:
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
    return reactions

# Генерация репостов
def generate_shares(user_ids: List[str], posts: List[Dict], n=10) -> List[Dict]:
    shares = []
    for i in range(n):
        post = random.choice(posts)
        shares.append({
            'id': i + 1,
            'post_id': post['id'],
            'user_id': random.choice(user_ids),
            'created_at': fake.date_between(start_date='-1y', end_date='today').strftime("%Y.%m.%d %H:%M")
        })
    return shares

# Финальная генерация всех данных
def generate_all_data() -> Dict[str, List[Dict]]:
    
    users = gen_user()
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
#     data = generate_all_data()  # <-- В data НЕТ информации о пользователях

#     # Вывод первых 2 записей каждого типа
#     for key, value in data.items():
#         print(f"\n{key.upper()}:")
#         for item in value:
#             print(item)