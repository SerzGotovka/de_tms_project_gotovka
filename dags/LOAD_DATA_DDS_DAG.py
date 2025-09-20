import os
import time
from airflow.providers.postgres.operators.postgres import PostgresOperator  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow import DAG  # type: ignore
from datetime import datetime, timedelta
import logging
from utils.function_minio import (
    load_from_raw_to_dds,
    load_photos_from_minio_to_postgres,
    load_videos_from_minio_to_postgres,
    load_albums_from_minio_to_postgres,
    load_posts_from_minio_to_postgres,
    load_stories_from_minio_to_postgres,
    load_reels_from_minio_to_postgres,
    load_comments_from_minio_to_postgres,
    load_replies_from_minio_to_postgres,
    load_likes_from_minio_to_postgres,
    load_reactions_from_minio_to_postgres,
    load_shares_from_minio_to_postgres
)
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


logger = logging.getLogger(__name__)


def build_dds_layer():

    # Создание таблиц в dds слое (для users, content, groups, media, social)
    SQL_DIR = os.path.join(os.path.dirname(__file__), "models", "sql", "dds")
    table_creation_order = [
        "users_tables",
        "content_tables",
        "groups_tables",
        "media_tables",
        "social_tables",        
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


# ==================== SOCIAL DATA FUNCTIONS ====================

def load_friends_data():
    """Загрузка данных дружеских связей из raw в DDS слой"""
    from models.pydantic.social_pydantic import Friend
    return load_from_raw_to_dds(
        prefix="social_friends/",
        table_name="friends",
        pydantic_model_class=Friend,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


def load_followers_data():
    """Загрузка данных подписчиков из raw в DDS слой"""
    from models.pydantic.social_pydantic import Follower
    return load_from_raw_to_dds(
        prefix="social_followers/",
        table_name="followers",
        pydantic_model_class=Follower,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


def load_subscriptions_data():
    """Загрузка данных подписок из raw в DDS слой"""
    from models.pydantic.social_pydantic import Subscription
    return load_from_raw_to_dds(
        prefix="social_subscriptions/",
        table_name="subscriptions",
        pydantic_model_class=Subscription,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


def load_blocks_data():
    """Загрузка данных блокировок из raw в DDS слой"""
    from models.pydantic.social_pydantic import Block
    return load_from_raw_to_dds(
        prefix="social_blocks/",
        table_name="blocks",
        pydantic_model_class=Block,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


def load_mutes_data():
    """Загрузка данных отключенных уведомлений из raw в DDS слой"""
    from models.pydantic.social_pydantic import Mute
    return load_from_raw_to_dds(
        prefix="social_mutes/",
        table_name="mutes",
        pydantic_model_class=Mute,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


def load_close_friends_data():
    """Загрузка данных близких друзей из raw в DDS слой"""
    from models.pydantic.social_pydantic import CloseFriend
    return load_from_raw_to_dds(
        prefix="social_close_friends/",
        table_name="close_friends",
        pydantic_model_class=CloseFriend,
        bucket_name="data-bucket",
        postgres_conn_id="my_postgres_conn",
        update_existing=True
    )


default_args = {
    "retries": 3,  # попробовать 3 раза при неудаче
    "retry_delay": timedelta(minutes=60)    
}

with DAG(
    dag_id="load_from_raw_to_dds",
    tags=["dds", "postgres", "NEO4J", "from_minio"],
    description="Даг для загрузки данных из MinIO в dds слой PostgreSQL и Neo4j",
    default_args=default_args,
    schedule_interval='*/3 * * * *',  # Запуск каждые  3 минуты для непрерывной обработки
    start_date=datetime(2025, 8, 4),
    catchup=False,
    max_active_runs=3  # Разрешить до 3 параллельных запусков для консюмеров
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

    # ==================== SOCIAL DATA LOADING TASKS ====================
    
    # Загрузка социальных данных из MinIO в DDS слой
    load_friends_data_task = PythonOperator(
        task_id="load_friends_data",
        python_callable=load_friends_data,
    )

    load_followers_data_task = PythonOperator(
        task_id="load_followers_data",
        python_callable=load_followers_data,
    )

    load_subscriptions_data_task = PythonOperator(
        task_id="load_subscriptions_data",
        python_callable=load_subscriptions_data,
    )

    load_blocks_data_task = PythonOperator(
        task_id="load_blocks_data",
        python_callable=load_blocks_data,
    )

    load_mutes_data_task = PythonOperator(
        task_id="load_mutes_data",
        python_callable=load_mutes_data,
    )

    load_close_friends_data_task = PythonOperator(
        task_id="load_close_friends_data",
        python_callable=load_close_friends_data,
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

    # ==================== MEDIA MINIO LOAD TASKS ====================
    
    load_photos_from_minio_task = PythonOperator(
        task_id="load_photos_from_minio",
        python_callable=load_photos_from_minio_to_postgres,
    )

    load_videos_from_minio_task = PythonOperator(
        task_id="load_videos_from_minio",
        python_callable=load_videos_from_minio_to_postgres,
    )

    load_albums_from_minio_task = PythonOperator(
        task_id="load_albums_from_minio",
        python_callable=load_albums_from_minio_to_postgres,
    )

    # ==================== CONTENT MINIO LOAD TASKS ====================
    
    load_posts_from_minio_task = PythonOperator(
        task_id="load_posts_from_minio",
        python_callable=load_posts_from_minio_to_postgres,
    )

    load_stories_from_minio_task = PythonOperator(
        task_id="load_stories_from_minio",
        python_callable=load_stories_from_minio_to_postgres,
    )

    load_reels_from_minio_task = PythonOperator(
        task_id="load_reels_from_minio",
        python_callable=load_reels_from_minio_to_postgres,
    )

    load_comments_from_minio_task = PythonOperator(
        task_id="load_comments_from_minio",
        python_callable=load_comments_from_minio_to_postgres,
    )

    load_replies_from_minio_task = PythonOperator(
        task_id="load_replies_from_minio",
        python_callable=load_replies_from_minio_to_postgres,
    )

    load_likes_from_minio_task = PythonOperator(
        task_id="load_likes_from_minio",
        python_callable=load_likes_from_minio_to_postgres,
    )

    load_reactions_from_minio_task = PythonOperator(
        task_id="load_reactions_from_minio",
        python_callable=load_reactions_from_minio_to_postgres,
    )

    load_shares_from_minio_task = PythonOperator(
        task_id="load_shares_from_minio",
        python_callable=load_shares_from_minio_to_postgres,
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
    
    # Загрузка социальных данных после загрузки пользователей
    load_users_task >> [
        load_friends_data_task,
        load_followers_data_task,
        load_subscriptions_data_task,
        load_blocks_data_task,
        load_mutes_data_task,
        load_close_friends_data_task,
    ]
    
    # Загрузка пользователей в Neo4j после загрузки в PostgreSQL
    load_users_task >> load_users_to_neo4j_task
    
    # Загрузка социальных связей в Neo4j после загрузки пользователей в Neo4j и социальных данных в DDS
    load_users_to_neo4j_task >> [
        load_friends_to_neo4j_task,
        load_followers_to_neo4j_task,
        load_subscriptions_to_neo4j_task,
        load_blocks_to_neo4j_task,
        load_mutes_to_neo4j_task,
        load_close_friends_to_neo4j_task,
        load_all_social_relationships_to_neo4j_task,
    ]
    
    # Социальные данные в Neo4j зависят от загрузки социальных данных в DDS
    load_friends_data_task >> load_friends_to_neo4j_task
    load_followers_data_task >> load_followers_to_neo4j_task
    load_subscriptions_data_task >> load_subscriptions_to_neo4j_task
    load_blocks_data_task >> load_blocks_to_neo4j_task
    load_mutes_data_task >> load_mutes_to_neo4j_task
    load_close_friends_data_task >> load_close_friends_to_neo4j_task
    
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

    # Media данные из MinIO зависят от загрузки пользователей
    load_users_task >> [
        load_photos_from_minio_task,
        load_videos_from_minio_task,
        load_albums_from_minio_task,
    ]

    # Content данные из MinIO зависят от загрузки пользователей
    load_users_task >> [
        load_posts_from_minio_task,
        load_stories_from_minio_task,
        load_reels_from_minio_task,
    ]

    # Комментарии и взаимодействия зависят от постов
    load_posts_from_minio_task >> [
        load_comments_from_minio_task,
        load_likes_from_minio_task,
        load_reactions_from_minio_task,
        load_shares_from_minio_task,
    ]

    # Ответы на комментарии зависят от комментариев
    load_comments_from_minio_task >> load_replies_from_minio_task

    # ==================== DEPENDENCIES ====================
    