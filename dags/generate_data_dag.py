from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import logging
from generate_data.generate_social_links import generate_blocks, generate_close_friends, generate_followers, generate_friends, generate_mutes, generate_subscriptions
from generate_data.generate_media import generate_albums, generate_photos, generate_videos
from generate_data.generate_content import generate_comments, generate_likes, generate_posts, generate_reactions, generate_reels, generate_replies, generate_shares, generate_stories
from generate_data.generate_group import generate_communities, generate_community_topics, generate_group_members, generate_groups, generate_pinned_posts
from generate_data.generate_users import gen_user, gen_user_profile, gen_user_settings, gen_user_privacy, gen_user_status


logger = logging.getLogger(__name__)

with DAG(
    dag_id='generate_data',
    start_date=None,#datetime(2025, 8, 4),
    schedule_interval=None,
    tags=['generate'],
    description='Даг для генерации всех данных',
    catchup=False
) as dag:
    
    def task_gen_users(**context):
        users = gen_user()
        context["task_instance"].xcom_push(key="users", value=users)
        return len(users)

    gen_users_task = PythonOperator(
        task_id="gen_users",
        python_callable=task_gen_users,
    )

    # 2. Генерация профилей
    def task_gen_profiles(**context):
        users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        profiles = gen_user_profile(users)
        context["task_instance"].xcom_push(key="profiles", value=profiles)
        return len(profiles)

    gen_profiles_task = PythonOperator(
        task_id="gen_profiles",
        python_callable=task_gen_profiles,
    )

    # 3. Генерация настроек
    def task_gen_settings(**context):
        users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        settings = gen_user_settings(users)
        context["task_instance"].xcom_push(key="settings", value=settings)
        return len(settings)

    gen_settings_task = PythonOperator(
        task_id="gen_settings",
        python_callable=task_gen_settings,
    )

    # 4. Генерация приватности
    def task_gen_privacy(**context):
        users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        privacies = gen_user_privacy(users)
        context["task_instance"].xcom_push(key="privacies", value=privacies)
        return len(privacies)

    gen_privacy_task = PythonOperator(
        task_id="gen_privacy",
        python_callable=task_gen_privacy,
    )

    # 5. Генерация статусов
    def task_gen_status(**context):
        users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        statuses = gen_user_status(users)
        context["task_instance"].xcom_push(key="statuses", value=statuses)
        return len(statuses)

    gen_status_task = PythonOperator(
        task_id="gen_status",
        python_callable=task_gen_status,
    )

#################groups#########################################   

    def task_gen_communities(**context):
        communities = generate_communities()
        context["task_instance"].xcom_push(key="communities", value=communities)
        return len(communities)

    gen_communities_task = PythonOperator(
        task_id="gen_communities",
        python_callable=task_gen_communities,
    )

    # # 4. Группы (требуют user_ids)
    def task_gen_groups(**context):
        users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        user_ids = [u["id"] for u in users]
        groups = generate_groups(user_ids)
        context["task_instance"].xcom_push(key="groups", value=groups)
        return len(groups)

    gen_groups_task = PythonOperator(
        task_id="gen_groups",
        python_callable=task_gen_groups,
    )

    # # 5. Участники групп
    def task_gen_group_members(**context):
        users   = context["task_instance"].xcom_pull(key="users",   task_ids="gen_users")
        groups  = context["task_instance"].xcom_pull(key="groups",  task_ids="gen_groups")
        user_ids  = [u["id"] for u in users]
        group_ids = [g["id"] for g in groups]
        members = generate_group_members(group_ids, user_ids)
        context["task_instance"].xcom_push(key="group_members", value=members)
        return len(members)

    gen_members_task = PythonOperator(
        task_id="gen_group_members",
        python_callable=task_gen_group_members,
    )

    # # 6. Темы сообществ
    def task_gen_community_topics(**context):
        communities = context["task_instance"].xcom_pull(
            key="communities", task_ids="gen_communities"
        )
        topics = generate_community_topics(communities)
        context["task_instance"].xcom_push(key="community_topics", value=topics)
        return len(topics)

    gen_topics_task = PythonOperator(
        task_id="gen_community_topics",
        python_callable=task_gen_community_topics,
    )

    # # 7. Закреплённые посты
    def task_gen_pinned_posts(**context):
        users       = context["task_instance"].xcom_pull(key="users",       task_ids="gen_users")
        communities = context["task_instance"].xcom_pull(key="communities", task_ids="gen_communities")
        groups      = context["task_instance"].xcom_pull(key="groups",      task_ids="gen_groups")
        user_ids = [u["id"] for u in users]
        pinned_posts = generate_pinned_posts(communities, groups, user_ids, n=5)
        context["task_instance"].xcom_push(key="pinned_posts", value=pinned_posts)
        return len(pinned_posts)

    gen_pinned_posts_task = PythonOperator(
        task_id="gen_pinned_posts",
        python_callable=task_gen_pinned_posts,
    )

#################################content###############################################################
# ---------- Новые задачи ----------
    def task_gen_content(**context):
        users   = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        user_ids = [u["id"] for u in users]

        posts      = generate_posts(user_ids)
        stories    = generate_stories(user_ids)
        reels      = generate_reels(user_ids)
        comments   = generate_comments(user_ids, posts)
        replies    = generate_replies(user_ids, comments)
        likes      = generate_likes(user_ids, posts)
        reactions  = generate_reactions(user_ids, posts)
        shares     = generate_shares(user_ids, posts)

        context["task_instance"].xcom_push(key="content", value={
            "posts": posts, "stories": stories, "reels": reels,
            "comments": comments, "replies": replies,
            "likes": likes, "reactions": reactions, "shares": shares
        })
        return len(posts) + len(stories) + len(reels)

    gen_content_task = PythonOperator(
        task_id="gen_content",
        python_callable=task_gen_content,
    )

############################media############################################    


    def task_gen_photos(**context):
        users  = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        photos = generate_photos(users, num_photos=10)
        context["task_instance"].xcom_push(key="photos", value=photos)
        return len(photos)

    def task_gen_videos(**context):
        users  = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        videos = generate_videos(users, num_videos=5)
        context["task_instance"].xcom_push(key="videos", value=videos)
        return len(videos)

    def task_gen_albums(**context):
        users  = context["task_instance"].xcom_pull(key="users",  task_ids="gen_users")
        photos = context["task_instance"].xcom_pull(key="photos", task_ids="gen_photos")
        videos = context["task_instance"].xcom_pull(key="videos", task_ids="gen_videos")
        albums = generate_albums(users, photos, videos, num_albums=3)
        context["task_instance"].xcom_push(key="albums", value=albums)
        return len(albums)

    gen_photos_task = PythonOperator(task_id="gen_photos", python_callable=task_gen_photos)
    gen_videos_task = PythonOperator(task_id="gen_videos", python_callable=task_gen_videos)
    gen_albums_task = PythonOperator(task_id="gen_albums", python_callable=task_gen_albums)

##################################social links#####################################################

    def task_generate_friends(**context):
        users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        friends = generate_friends(users, max_friends=3)
        context["task_instance"].xcom_push(key="friends", value=friends)
        return len(friends)

    def task_generate_followers(**context):
        users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        followers = generate_followers(users, max_followers=3)
        context["task_instance"].xcom_push(key="followers", value=followers)
        return len(followers)

    def task_generate_subscriptions(**context):
        users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        subscriptions = generate_subscriptions(users, max_subscriptions=3)
        context["task_instance"].xcom_push(key="subscriptions", value=subscriptions)
        return len(subscriptions)

    def task_generate_blocks(**context):
        users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        blocks = generate_blocks(users, max_blocks=2)
        context["task_instance"].xcom_push(key="blocks", value=blocks)
        return len(blocks)

    def task_generate_mutes(**context):
        users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        mutes = generate_mutes(users, max_mutes=2)
        context["task_instance"].xcom_push(key="mutes", value=mutes)
        return len(mutes)

    def task_generate_close_friends(**context):
        friends = context["task_instance"].xcom_pull(key="friends", task_ids="gen_friends")
        close_friends = generate_close_friends(friends)
        context["task_instance"].xcom_push(key="close_friends", value=close_friends)
        return len(close_friends)

    # ---------- задачи в DAG ----------
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
    gen_users_task >> gen_groups_task >> gen_photos_task >> gen_videos_task >> gen_friends_task >> gen_communities_task >> [
        gen_profiles_task,
        gen_settings_task,
        gen_privacy_task,
        gen_status_task,
        gen_members_task, gen_topics_task, gen_pinned_posts_task, gen_content_task, gen_albums_task, gen_followers_task, gen_subscriptions_task, gen_blocks_task, gen_mutes_task, gen_close_friends_task]
    


