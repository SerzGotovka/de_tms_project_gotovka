import os
from airflow.providers.postgres.operators.postgres import PostgresOperator  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow import DAG  # type: ignore
from datetime import datetime, timedelta
import logging
from utils.function_minio import load_from_raw_to_dds
from utils.load_to_neo_users import insert_users_to_neo4j
from utils.load_to_neo_social import (
    load_friends_to_neo4j,
    load_followers_to_neo4j,
    load_subscriptions_to_neo4j,
    load_blocks_to_neo4j,
    load_mutes_to_neo4j,
    load_close_friends_to_neo4j,
    load_all_social_relationships_to_neo4j
)
from models.pydantic.users_pydantic import User, UserProfile, UserSettings, UserPrivacy, UserStatusModel
from models.pydantic.groups_pydantic import Community, Group, GroupMember, CommunityTopic, PinnedPost

from models.pydantic.pydantic_models import (
    Photo, Video, Album, Post, Story, Reel, Comment, Reply, Like, Reaction, Share
)
# from kafka_consumer import consume_l_available_topics
# from kafka_consumer import kafka_consumer
from cons import (
    universal_insert_into_postgres, 
    universal_kafka_consumer
)


logger = logging.getLogger(__name__)


def build_dds_layer():

    # Создание таблиц в dds слое (для users, content, groups, media)
    SQL_DIR = os.path.join(os.path.dirname(__file__), "models", "sql", "dds")
    table_creation_order = [
        "users_tables",
        "content_tables",
        "groups_tables",
        "media_tables",        
    ]

    tasks = []
    prev_task = None
    for table_name in table_creation_order:
        # Найти путь к файлику
        sql_file_path = os.path.join(SQL_DIR, f"create_{table_name}.sql")
        relative_sql_path = os.path.relpath(sql_file_path, os.path.dirname(__file__))

        # Созданиие оператора создания таблички
        create_table_task = PostgresOperator(
            task_id=f"create_table_{table_name}",
            postgres_conn_id="my_postgres_conn",
            sql=relative_sql_path,
        )

        # Логика запуска
        if prev_task:
            prev_task >> create_table_task

        prev_task = create_table_task
        tasks.append(create_table_task)
    
    return tasks


def load_users_data():
    """Загрузка данных пользователей из raw в DDS слой"""
    return load_from_raw_to_dds(
        prefix="users/",
        table_name="users",
        pydantic_model_class=User,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True  # Обновляем существующие записи
    )


def load_user_profiles_data():
    """Загрузка профилей пользователей из raw в DDS слой"""
    return load_from_raw_to_dds(
        prefix="users_profiles/",
        table_name="user_profiles",
        pydantic_model_class=UserProfile,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


def load_user_settings_data():
    """Загрузка настроек пользователей из raw в DDS слой"""
    return load_from_raw_to_dds(
        prefix="users_settings/",
        table_name="user_settings",
        pydantic_model_class=UserSettings,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


def load_user_privacy_data():
    """Загрузка настроек приватности пользователей из raw в DDS слой"""
    return load_from_raw_to_dds(
        prefix="users_privacy/",
        table_name="user_privacy",
        pydantic_model_class=UserPrivacy,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


def load_user_status_data():
    """Загрузка статусов пользователей из raw в DDS слой"""
    return load_from_raw_to_dds(
        prefix="users_status/",
        table_name="user_status",
        pydantic_model_class=UserStatusModel,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


def load_communities_data():
    """Загрузка сообществ из raw в DDS слой"""
    return load_from_raw_to_dds(
        prefix="groups_communities/",
        table_name="communities",
        pydantic_model_class=Community,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


def load_groups_data():
    """Загрузка групп из raw в DDS слой"""
    return load_from_raw_to_dds(
        prefix="groups/",
        table_name="groups",
        pydantic_model_class=Group,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


def load_group_members_data():
    """Загрузка участников групп из raw в DDS слой"""
    return load_from_raw_to_dds(
        prefix="group_members/",
        table_name="group_members",
        pydantic_model_class=GroupMember,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


def load_community_topics_data():
    """Загрузка тем сообществ из raw в DDS слой"""
    return load_from_raw_to_dds(
        prefix="community_topics/",
        table_name="community_topics",
        pydantic_model_class=CommunityTopic,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


def load_pinned_posts_data():
    """Загрузка закрепленных постов из raw в DDS слой"""
    return load_from_raw_to_dds(
        prefix="pinned_posts/",
        table_name="pinned_posts",
        pydantic_model_class=PinnedPost,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True  # Обновляем существующие записи
    )


# ==================== MEDIA DATA FUNCTIONS ====================

def insert_photo_data(data: Photo) -> bool:
    """Вставка данных фотографии в PostgreSQL"""
    return universal_insert_into_postgres(
        data=data,
        table_name="photos",
        schema="dds",
        check_dependency={"users": "user_id"},
        insert_columns=["id", "user_id", "filename", "url", "description", "uploaded_at", "is_private"],
        insert_values=[data.id, data.user_id, data.filename, data.url, data.description, data.uploaded_at, data.is_private]
    )


def insert_video_data(data: Video) -> bool:
    """Вставка данных видео в PostgreSQL"""
    return universal_insert_into_postgres(
        data=data,
        table_name="videos",
        schema="dds",
        check_dependency={"users": "user_id"},
        insert_columns=["id", "user_id", "title", "url", "duration_seconds", "uploaded_at", "visibility"],
        insert_values=[data.id, data.user_id, data.title, data.url, data.duration_seconds, data.uploaded_at, data.visibility.value]
    )


def insert_album_data(data: Album) -> bool:
    """Вставка данных альбома в PostgreSQL"""
    return universal_insert_into_postgres(
        data=data,
        table_name="albums",
        schema="dds",
        check_dependency={"users": "user_id"},
        insert_columns=["id", "user_id", "title", "description", "created_at", "media_ids"],
        insert_values=[data.id, data.user_id, data.title, data.description, data.created_at, data.media_ids]
    )


def consume_photos_from_kafka() -> int:
    """Потребление фотографий из Kafka"""
    return universal_kafka_consumer(
        topics="media_photos",
        group_id="photos_consumer_group",
        data_model=Photo,
        insert_function=insert_photo_data,
        max_runtime=300,
        max_messages=100
    )


def consume_videos_from_kafka() -> int:
    """Потребление видео из Kafka"""
    return universal_kafka_consumer(
        topics="media_videos",
        group_id="videos_consumer_group",
        data_model=Video,
        insert_function=insert_video_data,
        max_runtime=300,
        max_messages=100
    )


def consume_albums_from_kafka() -> int:
    """Потребление альбомов из Kafka"""
    return universal_kafka_consumer(
        topics="media_albums",
        group_id="albums_consumer_group",
        data_model=Album,
        insert_function=insert_album_data,
        max_runtime=300,
        max_messages=100
    )


# ==================== CONTENT DATA FUNCTIONS ====================

def insert_post_data(data: Post) -> bool:
    """Вставка данных поста в PostgreSQL"""
    return universal_insert_into_postgres(
        data=data,
        table_name="posts",
        schema="dds",
        check_dependency={"users": "author_id"},
        insert_columns=["id", "author_id", "caption", "image_url", "location", "created_at"],
        insert_values=[data.id, data.author_id, data.caption, data.image_url, data.location, data.created_at]
    )


def insert_story_data(data: Story) -> bool:
    """Вставка данных истории в PostgreSQL"""
    return universal_insert_into_postgres(
        data=data,
        table_name="stories",
        schema="dds",
        check_dependency={"users": "user_id"},
        insert_columns=["id", "user_id", "image_url", "caption", "expires_at"],
        insert_values=[data.id, data.user_id, data.image_url, data.caption, data.expires_at]
    )


def insert_reel_data(data: Reel) -> bool:
    """Вставка данных Reels в PostgreSQL"""
    return universal_insert_into_postgres(
        data=data,
        table_name="reels",
        schema="dds",
        check_dependency={"users": "user_id"},
        insert_columns=["id", "user_id", "video_url", "caption", "music", "created_at"],
        insert_values=[data.id, data.user_id, data.video_url, data.caption, data.music, data.created_at]
    )


def insert_comment_data(data: Comment) -> bool:
    """Вставка данных комментария в PostgreSQL"""
    return universal_insert_into_postgres(
        data=data,
        table_name="comments",
        schema="dds",
        check_dependency={"users": "user_id", "posts": "post_id"},
        insert_columns=["id", "post_id", "user_id", "text", "created_at"],
        insert_values=[data.id, data.post_id, data.user_id, data.text, data.created_at]
    )


def insert_reply_data(data: Reply) -> bool:
    """Вставка данных ответа на комментарий в PostgreSQL"""
    return universal_insert_into_postgres(
        data=data,
        table_name="replies",
        schema="dds",
        check_dependency={"users": "user_id", "comments": "comment_id"},
        insert_columns=["id", "comment_id", "user_id", "text", "created_at"],
        insert_values=[data.id, data.comment_id, data.user_id, data.text, data.created_at]
    )


def insert_like_data(data: Like) -> bool:
    """Вставка данных лайка в PostgreSQL"""
    return universal_insert_into_postgres(
        data=data,
        table_name="likes",
        schema="dds",
        check_dependency={"users": "user_id", "posts": "post_id"},
        insert_columns=["id", "post_id", "user_id", "created_at"],
        insert_values=[data.id, data.post_id, data.user_id, data.created_at]
    )


def insert_reaction_data(data: Reaction) -> bool:
    """Вставка данных реакции в PostgreSQL"""
    return universal_insert_into_postgres(
        data=data,
        table_name="reactions",
        schema="dds",
        check_dependency={"users": "user_id", "posts": "post_id"},
        insert_columns=["id", "post_id", "user_id", "type", "created_at"],
        insert_values=[data.id, data.post_id, data.user_id, data.type.value, data.created_at]
    )


def insert_share_data(data: Share) -> bool:
    """Вставка данных репоста в PostgreSQL"""
    return universal_insert_into_postgres(
        data=data,
        table_name="shares",
        schema="dds",
        check_dependency={"users": "user_id", "posts": "post_id"},
        insert_columns=["id", "post_id", "user_id", "created_at"],
        insert_values=[data.id, data.post_id, data.user_id, data.created_at]
    )


def consume_posts_from_kafka() -> int:
    """Потребление постов из Kafka"""
    return universal_kafka_consumer(
        topics="content_posts",
        group_id="posts_consumer_group",
        data_model=Post,
        insert_function=insert_post_data,
        max_runtime=300,
        max_messages=100
    )


def consume_stories_from_kafka() -> int:
    """Потребление историй из Kafka"""
    return universal_kafka_consumer(
        topics="content_stories",
        group_id="stories_consumer_group",
        data_model=Story,
        insert_function=insert_story_data,
        max_runtime=300,
        max_messages=100
    )


def consume_reels_from_kafka() -> int:
    """Потребление Reels из Kafka"""
    return universal_kafka_consumer(
        topics="content_reels",
        group_id="reels_consumer_group",
        data_model=Reel,
        insert_function=insert_reel_data,
        max_runtime=300,
        max_messages=100
    )


def consume_comments_from_kafka() -> int:
    """Потребление комментариев из Kafka"""
    return universal_kafka_consumer(
        topics="content_comments",
        group_id="comments_consumer_group",
        data_model=Comment,
        insert_function=insert_comment_data,
        max_runtime=300,
        max_messages=100
    )


def consume_replies_from_kafka() -> int:
    """Потребление ответов на комментарии из Kafka"""
    return universal_kafka_consumer(
        topics="content_replies",
        group_id="replies_consumer_group",
        data_model=Reply,
        insert_function=insert_reply_data,
        max_runtime=300,
        max_messages=100
    )


def consume_likes_from_kafka() -> int:
    """Потребление лайков из Kafka"""
    return universal_kafka_consumer(
        topics="content_likes",
        group_id="likes_consumer_group",
        data_model=Like,
        insert_function=insert_like_data,
        max_runtime=300,
        max_messages=100
    )


def consume_reactions_from_kafka() -> int:
    """Потребление реакций из Kafka"""
    return universal_kafka_consumer(
        topics="content_reactions",
        group_id="reactions_consumer_group",
        data_model=Reaction,
        insert_function=insert_reaction_data,
        max_runtime=300,
        max_messages=100
    )


def consume_shares_from_kafka() -> int:
    """Потребление репостов из Kafka"""
    return universal_kafka_consumer(
        topics="content_shares",
        group_id="shares_consumer_group",
        data_model=Share,
        insert_function=insert_share_data,
        max_runtime=300,
        max_messages=100
    )


default_args = {
    "retries": 3,  # попробовать 3 раза при неудаче
    "retry_delay": timedelta(minutes=60),
    "schedule_interval": "@daily",
    "start_date": datetime(2025, 8, 4),
    "catchup": False,
    "max_active_runs": 1,
}

with DAG(
    dag_id="load_from_raw_to_dds",
    tags=["dds", "postgres", "NEO4J", "from_minio", "from_kafka"],
    description="Даг для загрузки данных из MinIO и kafka в dds слой PostgreSQL и Neo4j",
    default_args=default_args,
) as dag:
    # Создание таблиц DDS слоя
    dds_tables_tasks = build_dds_layer()

    # Загрузка данных пользователей
    load_users_task = PythonOperator(
        task_id="load_users_data",
        python_callable=load_users_data,
    )

    load_user_profiles_task = PythonOperator(
        task_id="load_user_profiles_data",
        python_callable=load_user_profiles_data,
    )

    load_user_settings_task = PythonOperator(
        task_id="load_user_settings_data",
        python_callable=load_user_settings_data,
    )

    load_user_privacy_task = PythonOperator(
        task_id="load_user_privacy_data",
        python_callable=load_user_privacy_data,
    )

    load_user_status_task = PythonOperator(
        task_id="load_user_status_data",
        python_callable=load_user_status_data,
    )

    # Загрузка пользователей в Neo4j
    load_users_to_neo4j_task = PythonOperator(
        task_id="load_users_to_neo4j",
        python_callable=insert_users_to_neo4j,
        provide_context=True,
    )

    # ==================== SOCIAL RELATIONSHIPS TO NEO4J ====================
    
    # Загрузка дружеских связей в Neo4j
    load_friends_to_neo4j_task = PythonOperator(
        task_id="load_friends_to_neo4j",
        python_callable=load_friends_to_neo4j,
        provide_context=True,
    )

    # Загрузка подписчиков в Neo4j
    load_followers_to_neo4j_task = PythonOperator(
        task_id="load_followers_to_neo4j",
        python_callable=load_followers_to_neo4j,
        provide_context=True,
    )

    # Загрузка подписок в Neo4j
    load_subscriptions_to_neo4j_task = PythonOperator(
        task_id="load_subscriptions_to_neo4j",
        python_callable=load_subscriptions_to_neo4j,
        provide_context=True,
    )

    # Загрузка блокировок в Neo4j
    load_blocks_to_neo4j_task = PythonOperator(
        task_id="load_blocks_to_neo4j",
        python_callable=load_blocks_to_neo4j,
        provide_context=True,
    )

    # Загрузка отключенных уведомлений в Neo4j
    load_mutes_to_neo4j_task = PythonOperator(
        task_id="load_mutes_to_neo4j",
        python_callable=load_mutes_to_neo4j,
        provide_context=True,
    )

    # Загрузка близких друзей в Neo4j
    load_close_friends_to_neo4j_task = PythonOperator(
        task_id="load_close_friends_to_neo4j",
        python_callable=load_close_friends_to_neo4j,
        provide_context=True,
    )

    # Загрузка всех социальных связей в Neo4j (альтернативная задача)
    load_all_social_relationships_to_neo4j_task = PythonOperator(
        task_id="load_all_social_relationships_to_neo4j",
        python_callable=load_all_social_relationships_to_neo4j,
        provide_context=True,
    )

    # Загрузка данных групп и сообществ
    load_communities_task = PythonOperator(
        task_id="load_communities_data",
        python_callable=load_communities_data,
    )

    load_groups_task = PythonOperator(
        task_id="load_groups_data",
        python_callable=load_groups_data,
    )

    load_group_members_task = PythonOperator(
        task_id="load_group_members_data",
        python_callable=load_group_members_data,
    )

    load_community_topics_task = PythonOperator(
        task_id="load_community_topics_data",
        python_callable=load_community_topics_data,
    )

    load_pinned_posts_task = PythonOperator(
        task_id="load_pinned_posts_data",
        python_callable=load_pinned_posts_data,
    )

    # ==================== MEDIA KAFKA CONSUMER TASKS ====================
    
    consume_photos_task = PythonOperator(
        task_id="consume_photos_from_kafka",
        python_callable=consume_photos_from_kafka,
    )

    consume_videos_task = PythonOperator(
        task_id="consume_videos_from_kafka",
        python_callable=consume_videos_from_kafka,
    )

    consume_albums_task = PythonOperator(
        task_id="consume_albums_from_kafka",
        python_callable=consume_albums_from_kafka,
    )

    # ==================== CONTENT KAFKA CONSUMER TASKS ====================
    
    consume_posts_task = PythonOperator(
        task_id="consume_posts_from_kafka",
        python_callable=consume_posts_from_kafka,
    )

    consume_stories_task = PythonOperator(
        task_id="consume_stories_from_kafka",
        python_callable=consume_stories_from_kafka,
    )

    consume_reels_task = PythonOperator(
        task_id="consume_reels_from_kafka",
        python_callable=consume_reels_from_kafka,
    )

    consume_comments_task = PythonOperator(
        task_id="consume_comments_from_kafka",
        python_callable=consume_comments_from_kafka,
    )

    consume_replies_task = PythonOperator(
        task_id="consume_replies_from_kafka",
        python_callable=consume_replies_from_kafka,
    )

    consume_likes_task = PythonOperator(
        task_id="consume_likes_from_kafka",
        python_callable=consume_likes_from_kafka,
    )

    consume_reactions_task = PythonOperator(
        task_id="consume_reactions_from_kafka",
        python_callable=consume_reactions_from_kafka,
    )

    consume_shares_task = PythonOperator(
        task_id="consume_shares_from_kafka",
        python_callable=consume_shares_from_kafka,
    )

    # Установка зависимостей
    # Сначала создаем таблицы, затем загружаем данные
    dds_tables_tasks[-1] >> load_users_task
    
    # Пользовательские данные должны загружаться в правильном порядке
    load_users_task >> [
        load_user_profiles_task,
        load_user_settings_task,
        load_user_privacy_task,
        load_user_status_task,
    ]
    
    # Загрузка пользователей в Neo4j после загрузки в PostgreSQL
    load_users_task >> load_users_to_neo4j_task
    
    # Загрузка социальных связей в Neo4j после загрузки пользователей в Neo4j
    load_users_to_neo4j_task >> [
        load_friends_to_neo4j_task,
        load_followers_to_neo4j_task,
        load_subscriptions_to_neo4j_task,
        load_blocks_to_neo4j_task,
        load_mutes_to_neo4j_task,
        load_close_friends_to_neo4j_task,
        load_all_social_relationships_to_neo4j_task,
    ]
    
    # Группы и сообщества загружаются параллельно после users
    load_users_task >> [
        load_communities_task,
        load_groups_task,
    ]
    
    # Групповые данные зависят от групп
    load_groups_task >> [
        load_group_members_task,
        load_community_topics_task,
        load_pinned_posts_task,
    ]

    # ==================== DEPENDENCIES ====================
    
    # Media Kafka consumers зависят от загрузки пользователей
    load_users_task >> [
        consume_photos_task,
        consume_videos_task,
        consume_albums_task,
    ]
    
    # Content Kafka consumers зависят от загрузки пользователей
    load_users_task >> [
        consume_posts_task,
        consume_stories_task,
        consume_reels_task,
    ]
    
    # Комментарии и взаимодействия зависят от постов
    consume_posts_task >> [
        consume_comments_task,
        consume_likes_task,
        consume_reactions_task,
        consume_shares_task,
    ]
    
    # Ответы на комментарии зависят от комментариев
    consume_comments_task >> consume_replies_task
    