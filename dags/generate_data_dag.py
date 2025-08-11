from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.utils.task_group import TaskGroup  # type: ignore
from datetime import datetime
import logging

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
    dag_id="generate_data",
    start_date=datetime(2025, 8, 4),
    schedule_interval="@daily",
    tags=["generate"],
    description="Даг для генерации всех данных",
    catchup=False,
    max_active_runs = 1
) as dag:
    
    ################################## ТАСКИ ДЛЯ USER ######################
    gen_users_task = PythonOperator(
        task_id="gen_users",
        python_callable=task_gen_users,
    )

    gen_profiles_task = PythonOperator(
        task_id="gen_profiles",
        python_callable=task_gen_profiles,
    )

    gen_settings_task = PythonOperator(
        task_id="gen_settings",
        python_callable=task_gen_settings,
    )

    gen_privacy_task = PythonOperator(
        task_id="gen_privacy",
        python_callable=task_gen_privacy,
    )

    gen_status_task = PythonOperator(
        task_id="gen_status",
        python_callable=task_gen_status,
    )
    ################################## ТАСКИ ДЛЯ GROUP ######################
    gen_communities_task = PythonOperator(
        task_id="gen_communities",
        python_callable=task_gen_communities,
    )

    gen_groups_task = PythonOperator(
        task_id="gen_groups",
        python_callable=task_gen_groups,
    )

    gen_members_task = PythonOperator(
        task_id="gen_group_members",
        python_callable=task_gen_group_members,
    )

    gen_topics_task = PythonOperator(
        task_id="gen_community_topics",
        python_callable=task_gen_community_topics,
    )

    gen_pinned_posts_task = PythonOperator(
        task_id="gen_pinned_posts",
        python_callable=task_gen_pinned_posts,
    )
    ################################## ТАСКИ ДЛЯ CONTENT ######################
    gen_content_task = PythonOperator(
        task_id="gen_content",
        python_callable=task_gen_content,
    )
    ################################## ТАСКИ ДЛЯ MEDIA ######################
    gen_photos_task = PythonOperator(
        task_id="gen_photos", python_callable=task_gen_photos
    )
    gen_videos_task = PythonOperator(
        task_id="gen_videos", python_callable=task_gen_videos
    )
    gen_albums_task = PythonOperator(
        task_id="gen_albums", python_callable=task_gen_albums
    )

    ################################## ТАСКИ ДЛЯ SOCIAL LINKS ######################

    gen_friends_task = PythonOperator(
        task_id="gen_friends",
        python_callable=task_generate_friends,
    )

    gen_followers_task = PythonOperator(
        task_id="gen_followers",
        python_callable=task_generate_followers,
    )

    gen_subscriptions_task = PythonOperator(
        task_id="gen_subscriptions",
        python_callable=task_generate_subscriptions,
    )

    gen_blocks_task = PythonOperator(
        task_id="gen_blocks",
        python_callable=task_generate_blocks,
    )

    gen_mutes_task = PythonOperator(
        task_id="gen_mutes",
        python_callable=task_generate_mutes,
    )

    gen_close_friends_task = PythonOperator(
        task_id="gen_close_friends",
        python_callable=task_generate_close_friends,
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
