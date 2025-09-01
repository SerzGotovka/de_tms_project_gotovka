from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.utils.task_group import TaskGroup  # type: ignore
from datetime import datetime
import logging
from utils.function_kafka import create_kafka_topics
from kafka_producer import send_media_to_kafka, send_content_to_kafka
from utils.function_for_dag import (
    task_gen_albums,
    task_gen_communities,
    task_gen_community_topics,
    task_gen_content,
    task_gen_group_members,
    task_gen_groups,
    task_gen_photos,
    task_gen_pinned_posts,
    task_gen_privacy,
    task_gen_profiles,
    task_gen_settings,
    task_gen_status,
    task_gen_users,
    task_gen_videos,
    task_generate_blocks,
    task_generate_close_friends,
    task_generate_followers,
    task_generate_friends,
    task_generate_mutes,
    task_generate_subscriptions,
)


logger = logging.getLogger(__name__)

with DAG(
    dag_id="total_generate_data",
    start_date=datetime(2025, 8, 4),
    schedule_interval="@daily",
    tags=["generate"],
    description="Даг для генерации всех данных",
    catchup=False,
    max_active_runs=1,
) as dag:
    create_kafka_topics_task = PythonOperator(
        task_id="create_kafka_topics",
        python_callable=create_kafka_topics,
        provide_context=True,
    )

    with TaskGroup(group_id="generate_data_group") as generate_data_group:
        ################################## ТАСКИ ДЛЯ USER ######################
        gen_users_task = PythonOperator(
            task_id="gen_users", python_callable=task_gen_users, provide_context=True
        )

        gen_profiles_task = PythonOperator(
            task_id="gen_profiles",
            python_callable=task_gen_profiles,
            provide_context=True,
        )

        gen_settings_task = PythonOperator(
            task_id="gen_settings",
            python_callable=task_gen_settings,
            provide_context=True,
        )

        gen_privacy_task = PythonOperator(
            task_id="gen_privacy",
            python_callable=task_gen_privacy,
            provide_context=True,
        )

        gen_status_task = PythonOperator(
            task_id="gen_status", python_callable=task_gen_status, provide_context=True
        )
        ################################## ТАСКИ ДЛЯ GROUP ######################
        gen_communities_task = PythonOperator(
            task_id="gen_communities",
            python_callable=task_gen_communities,
            provide_context=True,
        )

        gen_groups_task = PythonOperator(
            task_id="gen_groups", python_callable=task_gen_groups, provide_context=True
        )

        gen_members_task = PythonOperator(
            task_id="gen_group_members",
            python_callable=task_gen_group_members,
            provide_context=True,
        )

        gen_topics_task = PythonOperator(
            task_id="gen_community_topics",
            python_callable=task_gen_community_topics,
            provide_context=True,
        )

        gen_pinned_posts_task = PythonOperator(
            task_id="gen_pinned_posts",
            python_callable=task_gen_pinned_posts,
            provide_context=True,
        )
        ################################## ТАСКИ ДЛЯ CONTENT ######################
        gen_content_task = PythonOperator(
            task_id="gen_content",
            python_callable=task_gen_content,
            provide_context=True,
        )
        ################################## ТАСКИ ДЛЯ MEDIA ######################
        gen_photos_task = PythonOperator(
            task_id="gen_photos", python_callable=task_gen_photos, provide_context=True
        )
        gen_videos_task = PythonOperator(
            task_id="gen_videos", python_callable=task_gen_videos, provide_context=True
        )
        gen_albums_task = PythonOperator(
            task_id="gen_albums", python_callable=task_gen_albums, provide_context=True
        )

        ################################## ТАСКИ ДЛЯ SOCIAL LINKS ######################

        gen_friends_task = PythonOperator(
            task_id="gen_friends",
            python_callable=task_generate_friends,
            provide_context=True,
        )

        gen_followers_task = PythonOperator(
            task_id="gen_followers",
            python_callable=task_generate_followers,
            provide_context=True,
        )

        gen_subscriptions_task = PythonOperator(
            task_id="gen_subscriptions",
            python_callable=task_generate_subscriptions,
            provide_context=True,
        )

        gen_blocks_task = PythonOperator(
            task_id="gen_blocks",
            python_callable=task_generate_blocks,
            provide_context=True,
        )

        gen_mutes_task = PythonOperator(
            task_id="gen_mutes",
            python_callable=task_generate_mutes,
            provide_context=True,
        )

        gen_close_friends_task = PythonOperator(
            task_id="gen_close_friends",
            python_callable=task_generate_close_friends,
            provide_context=True,
        )

        # ---------- Зависимости ----------
        (
            gen_users_task
            >> gen_groups_task
            >> gen_photos_task
            >> gen_videos_task
            >> gen_friends_task
            >> gen_communities_task
            >> [
                gen_profiles_task,
                gen_settings_task,
                gen_privacy_task,
                gen_status_task,
                gen_members_task,
                gen_topics_task,
                gen_pinned_posts_task,
                gen_content_task,
                gen_albums_task,
                gen_followers_task,
                gen_subscriptions_task,
                gen_blocks_task,
                gen_mutes_task,
                gen_close_friends_task,
            ]
        )

    # Создаем задачи Kafka вне группы
    send_media_to_kafka_task = PythonOperator(
        task_id="send_media_to_kafka",
        python_callable=send_media_to_kafka,
        provide_context=True,
    )

    send_content_to_kafka_task = PythonOperator(
        task_id="send_content_to_kafka",
        python_callable=send_content_to_kafka,
        provide_context=True,
    )

    # Настраиваем зависимости
    (
        create_kafka_topics_task
        >> generate_data_group
        >> [send_media_to_kafka_task, send_content_to_kafka_task]
    )
