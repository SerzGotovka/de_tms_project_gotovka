from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.utils.task_group import TaskGroup  # type: ignore
from datetime import datetime, timedelta
import logging
import os
from generate_data.generate_users import gen_user, gen_user_profile, gen_user_settings, gen_user_privacy, gen_user_status
from generate_data.generate_group import generate_communities, generate_groups, generate_group_members, generate_community_topics, generate_pinned_posts
from generate_data.generate_social_links import generate_friends, generate_followers, generate_subscriptions, generate_blocks, generate_mutes, generate_close_friends, generate_social_data
from generate_data.generate_media import generate_photos, generate_videos, generate_albums
from generate_data.generate_content import generate_posts, generate_stories, generate_reels, generate_comments, generate_replies, generate_likes, generate_reactions, generate_shares


from utils.tg_bot import success_callback, on_failure_callback
from utils.function_minio import save_csv_file
from my_producer_media import my_producer_media, my_producer_albums
from my_producer_content import (
    my_producer_posts, my_producer_stories, my_producer_reels,
    my_producer_comments, my_producer_replies, my_producer_likes,
    my_producer_reactions, my_producer_shares
)

logger = logging.getLogger(__name__)


default_args = {
    'retries': 3,  # попробовать 3 раза при неудаче
    'retry_delay': timedelta(minutes=60),
    'schedule_interval': '@daily',
    'start_date': datetime(2025, 8, 4),
    'catchup': False,
    'max_active_runs': 1
}

with DAG(
    dag_id="total_generate_data",    
    tags=["generate", "save_csv_to_minio", "save_to_kafka"],
    description="Даг для генерации данных пользователей, групп и социальных связей. Сохраняет данные в CSV файлы в MinIO и отправляет(content и media) в Kafka",
    default_args=default_args
    
) as dag:
    

    with TaskGroup(group_id="generate_data_group") as generate_data_group:
        ################################## ТАСКИ ДЛЯ USER ######################
        gen_users_task = PythonOperator(
            task_id="gen_users", python_callable=gen_user, provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_profiles_task = PythonOperator(
            task_id="gen_profiles",
            python_callable=gen_user_profile,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_settings_task = PythonOperator(
            task_id="gen_settings",
            python_callable= gen_user_settings, 
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_privacy_task = PythonOperator(
            task_id="gen_privacy",
            python_callable=gen_user_privacy, 
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )
# 
        gen_status_task = PythonOperator(
            task_id="gen_status", python_callable=gen_user_status, 
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )
        ################################## ТАСКИ ДЛЯ GROUP ######################
        gen_communities_task = PythonOperator(
            task_id="gen_communities",
            python_callable=generate_communities,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_groups_task = PythonOperator(
            task_id="gen_groups", python_callable=generate_groups, provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_members_task = PythonOperator(
            task_id="gen_group_members",
            python_callable=generate_group_members,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_topics_task = PythonOperator(
            task_id="gen_community_topics",
            python_callable=generate_community_topics,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_pinned_posts_task = PythonOperator(
            task_id="gen_pinned_posts",
            python_callable=generate_pinned_posts,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )
        # ################################## ТАСКИ ДЛЯ CONTENT ######################
        gen_posts_task = PythonOperator(
            task_id="gen_posts",
            python_callable=generate_posts,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_stories_task = PythonOperator(
            task_id="gen_stories",
            python_callable=generate_stories,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_reels_task = PythonOperator(
            task_id="gen_reels",
            python_callable=generate_reels,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_comments_task = PythonOperator(
            task_id="gen_comments",
            python_callable=generate_comments,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_replies_task = PythonOperator(
            task_id="gen_replies",
            python_callable=generate_replies,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_likes_task = PythonOperator(
            task_id="gen_likes",
            python_callable=generate_likes,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_reactions_task = PythonOperator(
            task_id="gen_reactions",
            python_callable=generate_reactions,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_shares_task = PythonOperator(
            task_id="gen_shares",
            python_callable=generate_shares,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )
        # ################################## ТАСКИ ДЛЯ MEDIA ######################
        gen_photos_task = PythonOperator(
            task_id="gen_photos", python_callable=generate_photos, provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )
        gen_videos_task = PythonOperator(
            task_id="gen_videos", python_callable=generate_videos, provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )
        gen_albums_task = PythonOperator(
            task_id="gen_albums", python_callable=generate_albums, provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        # ################################## ТАСКИ ДЛЯ SOCIAL LINKS ######################

        gen_friends_task = PythonOperator(
            task_id="gen_friends",
            python_callable=generate_friends,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_followers_task = PythonOperator(
            task_id="gen_followers",
            python_callable=generate_followers,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_subscriptions_task = PythonOperator(
            task_id="gen_subscriptions",
            python_callable=generate_subscriptions,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_blocks_task = PythonOperator(
            task_id="gen_blocks",
            python_callable=generate_blocks,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_mutes_task = PythonOperator(
            task_id="gen_mutes",
            python_callable=generate_mutes,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_close_friends_task = PythonOperator(
            task_id="gen_close_friends",
            python_callable=generate_close_friends,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
        )

        gen_social_data_task = PythonOperator(
            task_id="gen_social_data",
            python_callable=generate_social_data,
            provide_context=True,
            on_success_callback=success_callback,
            on_failure_callback=on_failure_callback,
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
                gen_albums_task,
                gen_followers_task,
                gen_subscriptions_task,
                gen_blocks_task,
                gen_mutes_task,
                gen_close_friends_task,
                gen_posts_task,
                gen_stories_task,
                gen_reels_task,
            ]
        )

        # Зависимости для контента (посты должны быть созданы перед комментариями, лайками и т.д.)
        (
            gen_posts_task
            >> [gen_comments_task, gen_likes_task, gen_reactions_task, gen_shares_task]
            >> gen_replies_task
        )

        # Истории и Reels могут генерироваться параллельно с постами
        gen_stories_task
        gen_reels_task

    # Сохранение групп после генерации всех групповых данных
    [
        gen_communities_task,
        gen_members_task,
        gen_topics_task,
        gen_pinned_posts_task,
    ] 

    # Сохранение социальных данных после генерации всех социальных связей
    [
        gen_friends_task,
        gen_followers_task,
        gen_subscriptions_task,
        gen_blocks_task,
        gen_mutes_task,
        gen_close_friends_task,
        gen_social_data_task,
    ] 
    
    # === ЗАДАЧИ KAFKA ===
    
    # Задача для отправки медиа данных в Kafka
    send_photos_task = PythonOperator(
        task_id='send_photos_to_kafka',
        python_callable=my_producer_media,
        op_kwargs={'media_type': 'photos'},
        provide_context=True,
        dag=dag,
        on_success_callback=success_callback,
        on_failure_callback=on_failure_callback,
    )

    send_videos_task = PythonOperator(
        task_id='send_videos_to_kafka',
        python_callable=my_producer_media,
        op_kwargs={'media_type': 'videos'},
        provide_context=True,
        dag=dag,
        on_success_callback=success_callback,
        on_failure_callback=on_failure_callback,
    )

    send_albums_task = PythonOperator(
        task_id='send_albums_to_kafka',
        python_callable=my_producer_albums,
        op_kwargs={'media_type': 'albums'},
        provide_context=True,
        dag=dag,
        on_success_callback=success_callback,
        on_failure_callback=on_failure_callback,
    )

    # Задача для отправки постов в Kafka
    send_posts_task = PythonOperator(
        task_id='send_posts_to_kafka',
        python_callable=my_producer_posts,
        provide_context=True,
        dag=dag,
        on_success_callback=success_callback,
        on_failure_callback=on_failure_callback,
    )

    # Задача для отправки историй в Kafka
    send_stories_task = PythonOperator(
        task_id='send_stories_to_kafka',
        python_callable=my_producer_stories,
        provide_context=True,
        dag=dag,
        on_success_callback=success_callback,
        on_failure_callback=on_failure_callback,
    )

    # Задача для отправки reels в Kafka
    send_reels_task = PythonOperator(
        task_id='send_reels_to_kafka',
        python_callable=my_producer_reels,
        provide_context=True,
        dag=dag,
        on_success_callback=success_callback,
        on_failure_callback=on_failure_callback,
    )

    # Задача для отправки комментариев в Kafka
    send_comments_task = PythonOperator(
        task_id='send_comments_to_kafka',
        python_callable=my_producer_comments,
        provide_context=True,
        dag=dag,
        on_success_callback=success_callback,
        on_failure_callback=on_failure_callback,
    )

    # Задача для отправки ответов на комментарии в Kafka
    send_replies_task = PythonOperator(
        task_id='send_replies_to_kafka',
        python_callable=my_producer_replies,
        provide_context=True,
        dag=dag,
        on_success_callback=success_callback,
        on_failure_callback=on_failure_callback,
    )

    # Задача для отправки лайков в Kafka
    send_likes_task = PythonOperator(
        task_id='send_likes_to_kafka',
        python_callable=my_producer_likes,
        provide_context=True,
        dag=dag,
        on_success_callback=success_callback,
        on_failure_callback=on_failure_callback,
    )

    # Задача для отправки реакций в Kafka
    send_reactions_task = PythonOperator(
        task_id='send_reactions_to_kafka',
        python_callable=my_producer_reactions,
        provide_context=True,
        dag=dag,
        on_success_callback=success_callback,
        on_failure_callback=on_failure_callback,
    )

    # Задача для отправки репостов в Kafka
    send_shares_task = PythonOperator(
        task_id='send_shares_to_kafka',
        python_callable=my_producer_shares,
        provide_context=True,
        dag=dag,
        on_success_callback=success_callback,
        on_failure_callback=on_failure_callback,
    )
    
    # === ЗАВИСИМОСТИ KAFKA ===
    
    # Медиа данные зависят от генерации медиа
    gen_photos_task >> send_photos_task
    gen_videos_task >> send_videos_task
    gen_albums_task >> send_albums_task
    
    # Контент зависит от генерации контента
    gen_posts_task >> send_posts_task
    gen_stories_task >> send_stories_task
    gen_reels_task >> send_reels_task
    
    # Комментарии и ответы зависят от постов
    send_posts_task >> send_comments_task
    send_comments_task >> send_replies_task
    
    # Лайки, реакции и репосты зависят от постов
    send_posts_task >> send_likes_task
    send_posts_task >> send_reactions_task
    send_posts_task >> send_shares_task

