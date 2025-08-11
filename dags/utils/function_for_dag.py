from generate_data.generate_social_links import (
    generate_blocks,
    generate_close_friends,
    generate_followers,
    generate_friends,
    generate_mutes,
    generate_subscriptions,
)
from generate_data.generate_media import (
    generate_albums,
    generate_photos,
    generate_videos,
)
from generate_data.generate_content import (
    generate_comments,
    generate_likes,
    generate_posts,
    generate_reactions,
    generate_reels,
    generate_replies,
    generate_shares,
    generate_stories,
)
from generate_data.generate_group import (
    generate_communities,
    generate_community_topics,
    generate_group_members,
    generate_groups,
    generate_pinned_posts,
)
from generate_data.generate_users import (
    gen_user,
    gen_user_profile,
    gen_user_settings,
    gen_user_privacy,
    gen_user_status,
)
from utils.function import save_csv_file


def task_gen_users(**context):
    """Функция для генерации пользователей"""
    users = gen_user()
    context["task_instance"].xcom_push(key="users", value=users)
    return len(users)


def task_gen_profiles(**context):
    """Функция для генерации профилей"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    profiles = gen_user_profile(users)
    context["task_instance"].xcom_push(key="profiles", value=profiles)
    return len(profiles)


def task_gen_settings(**context):
    """Функция для генерации настроек пользователя"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    settings = gen_user_settings(users)
    context["task_instance"].xcom_push(key="settings", value=settings)
    return len(settings)


def task_gen_privacy(**context):
    """Функция для генерации приватности пользователя"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    privacies = gen_user_privacy(users)
    context["task_instance"].xcom_push(key="privacies", value=privacies)
    return len(privacies)


def task_gen_status(**context):
    """Функция для генерации статуса пользователя"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    statuses = gen_user_status(users)
    context["task_instance"].xcom_push(key="statuses", value=statuses)
    return len(statuses)


def task_gen_communities(**context):
    """Функция для генерации коммьюнити"""
    communities = generate_communities()
    
    # Сохраняем сообщества в CSV файл
    temp_file_path = '/opt/airflow/dags/save_data/groups/data_communities.csv'
    save_csv_file(temp_file_path, communities)
    
    context["task_instance"].xcom_push(key="communities", value=communities)
    return len(communities)


def task_gen_groups(**context):
    """Функция для генерации групп"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    user_ids = [u["id"] for u in users]
    groups = generate_groups(user_ids)
    
    # Сохраняем группы в CSV файл
    temp_file_path = '/opt/airflow/dags/save_data/groups/data_groups.csv'
    save_csv_file(temp_file_path, groups)
    
    context["task_instance"].xcom_push(key="groups", value=groups)
    return len(groups)


def task_gen_group_members(**context):
    """Функция для генерации участников группы"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    groups = context["task_instance"].xcom_pull(key="groups", task_ids="gen_groups")
    user_ids = [u["id"] for u in users]
    group_ids = [g["id"] for g in groups]
    members = generate_group_members(group_ids, user_ids)
    
    # Сохраняем участников групп в CSV файл
    temp_file_path = '/opt/airflow/dags/save_data/groups/data_group_members.csv'
    save_csv_file(temp_file_path, members)
    
    context["task_instance"].xcom_push(key="group_members", value=members)
    return len(members)


def task_gen_community_topics(**context):
    """Функция для генерации темы сообщества"""
    communities = context["task_instance"].xcom_pull(
        key="communities", task_ids="gen_communities"
    )
    topics = generate_community_topics(communities)
    context["task_instance"].xcom_push(key="community_topics", value=topics)
    return len(topics)


def task_gen_pinned_posts(**context):
    """Функция для генерации закрепленных постов"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    communities = context["task_instance"].xcom_pull(
        key="communities", task_ids="gen_communities"
    )
    groups = context["task_instance"].xcom_pull(key="groups", task_ids="gen_groups")
    user_ids = [u["id"] for u in users]
    pinned_posts = generate_pinned_posts(communities, groups, user_ids, n=5)
    context["task_instance"].xcom_push(key="pinned_posts", value=pinned_posts)
    return len(pinned_posts)


def task_gen_content(**context):
    """Функция для генерации контента"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    user_ids = [u["id"] for u in users]

    posts = generate_posts(user_ids)
    stories = generate_stories(user_ids)
    reels = generate_reels(user_ids)
    comments = generate_comments(user_ids, posts)
    replies = generate_replies(user_ids, comments)
    likes = generate_likes(user_ids, posts)
    reactions = generate_reactions(user_ids, posts)
    shares = generate_shares(user_ids, posts)

    context["task_instance"].xcom_push(
        key="content",
        value={
            "posts": posts,
            "stories": stories,
            "reels": reels,
            "comments": comments,
            "replies": replies,
            "likes": likes,
            "reactions": reactions,
            "shares": shares,
        },
    )
    return len(posts) + len(stories) + len(reels)


def task_gen_photos(**context):
    """Функция для генерации фото"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    photos = generate_photos(users, num_photos=10)
    context["task_instance"].xcom_push(key="photos", value=photos)
    return len(photos)


def task_gen_videos(**context):
    """Функция для генерации видео"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    videos = generate_videos(users, num_videos=5)
    context["task_instance"].xcom_push(key="videos", value=videos)
    return len(videos)


def task_gen_albums(**context):
    """Функция для генерации медиа-альбомов"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    photos = context["task_instance"].xcom_pull(key="photos", task_ids="gen_photos")
    videos = context["task_instance"].xcom_pull(key="videos", task_ids="gen_videos")
    albums = generate_albums(users, photos, videos, num_albums=3)
    context["task_instance"].xcom_push(key="albums", value=albums)
    return len(albums)


def task_generate_friends(**context):
    """Функция для генерации друзей"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    friends = generate_friends(users, max_friends=3)
    
    # Сохраняем друзей в CSV файл
    temp_file_path = '/opt/airflow/dags/save_data/social/data_social_friends.csv'
    save_csv_file(temp_file_path, friends)
    
    context["task_instance"].xcom_push(key="friends", value=friends)
    return len(friends)


def task_generate_followers(**context):
    """Функция для генерации подписчиков"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    followers = generate_followers(users, max_followers=3)
    
    # Сохраняем подписчиков в CSV файл
    temp_file_path = '/opt/airflow/dags/save_data/social/data_social_followers.csv'
    save_csv_file(temp_file_path, followers)
    
    context["task_instance"].xcom_push(key="followers", value=followers)
    return len(followers)


def task_generate_subscriptions(**context):
    """Функция для генерации подписок"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    subscriptions = generate_subscriptions(users, max_subscriptions=3)
    
    # Сохраняем подписки в CSV файл
    temp_file_path = '/opt/airflow/dags/save_data/social/data_social_subscriptions.csv'
    save_csv_file(temp_file_path, subscriptions)
    
    context["task_instance"].xcom_push(key="subscriptions", value=subscriptions)
    return len(subscriptions)


def task_generate_blocks(**context):
    """Функция для генерации заблокированных друзей"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    blocks = generate_blocks(users, max_blocks=2)
    
    # Сохраняем блокировки в CSV файл
    temp_file_path = '/opt/airflow/dags/save_data/social/data_social_blocks.csv'
    save_csv_file(temp_file_path, blocks)
    
    context["task_instance"].xcom_push(key="blocks", value=blocks)
    return len(blocks)


def task_generate_mutes(**context):
    users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
    mutes = generate_mutes(users, max_mutes=2)
    
    # Сохраняем отключенные уведомления в CSV файл
    temp_file_path = '/opt/airflow/dags/save_data/social/data_social_mutes.csv'
    save_csv_file(temp_file_path, mutes)
    
    context["task_instance"].xcom_push(key="mutes", value=mutes)
    return len(mutes)


def task_generate_close_friends(**context):
    """Функция для генерации близких друзей"""
    friends = context["task_instance"].xcom_pull(key="friends", task_ids="gen_friends")
    close_friends = generate_close_friends(friends)
    
    # Сохраняем близких друзей в CSV файл
    temp_file_path = '/opt/airflow/dags/save_data/social/data_social_close_friends.csv'
    save_csv_file(temp_file_path, close_friends)
    
    context["task_instance"].xcom_push(key="close_friends", value=close_friends)
    return len(close_friends)
