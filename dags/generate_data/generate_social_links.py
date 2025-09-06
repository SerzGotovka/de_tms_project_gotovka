from faker import Faker
import random
import uuid
from typing import List, Dict
import logging
from utils.function_minio import save_csv_file
from utils.config_generate import (temp_file_path_friends, temp_file_path_followers, temp_file_path_subscriptions,
                                   temp_file_path_blocks, temp_file_path_mutes, temp_file_path_close_friends)

fake = Faker()



def generate_friends(max_friends=3, **context) -> List[Dict]:
    """Генерация дружеских связей"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    friends = []
    for user in users:
        num_friends = random.randint(0, max_friends)
        potential_friends = [u for u in users if u["id"] != user["id"]]
        selected_friends = random.sample(
            potential_friends, min(num_friends, len(potential_friends))
        )

        for friend in selected_friends:
            friends.append(
                {
                    "id": str(uuid.uuid4()),
                    "user_id": user["id"],
                    "friend_id": friend["id"],
                    "created_at": fake.date_between(
                        start_date="-1y", end_date="today"
                    ).strftime("%Y.%m.%d %H:%M"),
                    "status": "active",
                    "is_best_friend": random.choice([True, False]),
                }
            )
    num_friends = len(friends)
    logging.info(f"Сгенерировано дружеских связей: {len(friends)}")
    logging.info(friends)

    context["task_instance"].xcom_push(key="friends", value=friends)
    context["task_instance"].xcom_push(key="num_friends", value=num_friends)
    save_csv_file(temp_file_path_friends, friends)
    return friends



def generate_followers(max_followers=3, **context) -> List[Dict]:
    """Генерация подписчиков"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    followers = []
    for user in users:
        num_followers = random.randint(0, max_followers)
        potential_followers = [u for u in users if u["id"] != user["id"]]
        selected_followers = random.sample(
            potential_followers, min(num_followers, len(potential_followers))
        )

        for follower in selected_followers:
            followers.append(
                {
                    "id": str(uuid.uuid4()),
                    "user_id": user["id"],
                    "follower_id": follower["id"],
                    "followed_at": fake.date_between(
                        start_date="-1y", end_date="today"
                    ).strftime("%Y.%m.%d %H:%M"),
                    "is_active": True,
                }
            )
    num_followers = len(followers)
    logging.info(f"Сгенерировано подписчиков: {len(followers)}")
    logging.info(followers)

    context["task_instance"].xcom_push(key="followers", value=followers)
    context["task_instance"].xcom_push(key="num_followers", value=num_followers)
    save_csv_file(temp_file_path_followers, followers)
    return followers



def generate_subscriptions(max_subscriptions=3, **context) -> List[Dict]:
    """Генерация подписок"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    subscriptions = []
    for user in users:
        num_subs = random.randint(0, max_subscriptions)
        potential_subs = [u for u in users if u["id"] != user["id"]]
        selected_subs = random.sample(
            potential_subs, min(num_subs, len(potential_subs))
        )

        for sub in selected_subs:
            subscriptions.append(
                {
                    "id": str(uuid.uuid4()),
                    "user_id": user["id"],
                    "subscribed_to": sub["id"],
                    "subscribed_at": fake.date_between(
                        start_date="-1y", end_date="today"
                    ).strftime("%Y.%m.%d %H:%M"),
                    "is_active": True,
                }
            )
    num_subscriptions = len(subscriptions)
    logging.info(f"Сгенерировано подписок: {len(subscriptions)}")
    logging.info(subscriptions)

    context["task_instance"].xcom_push(key="subscriptions", value=subscriptions)
    context["task_instance"].xcom_push(key="num_subscriptions", value=num_subscriptions)
    save_csv_file(temp_file_path_subscriptions, subscriptions)
    return subscriptions



def generate_blocks(max_blocks=2, **context) -> List[Dict]:
    """Генерация блокировок"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    blocks = []
    for user in users:
        num_blocks = random.randint(0, max_blocks)
        potential_blocks = [u for u in users if u["id"] != user["id"]]
        selected_blocks = random.sample(
            potential_blocks, min(num_blocks, len(potential_blocks))
        )

        for blocked in selected_blocks:
            blocks.append(
                {
                    "id": str(uuid.uuid4()),
                    "user_id": user["id"],
                    "blocked_id": blocked["id"],
                    "blocked_at": fake.date_between(
                        start_date="-1y", end_date="today"
                    ).strftime("%Y.%m.%d %H:%M"),
                    "reason": random.choice(
                        ["spam", "harassment", "unfollow", "other"]
                    ),
                    "is_active": True,
                }
            )
    num_blocks = len(blocks)
    logging.info(f"Сгенерировано блокировок: {len(blocks)}")
    logging.info(blocks)

    context["task_instance"].xcom_push(key="blocks", value=blocks)
    context["task_instance"].xcom_push(key="num_blocks", value=num_blocks)
    
    save_csv_file(temp_file_path_blocks, blocks)
    return blocks



def generate_mutes(max_mutes=2, **context) -> List[Dict]:
    """Генерация отключенных уведомлений (mutes)"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    mutes = []
    for user in users:
        num_mutes = random.randint(0, max_mutes)
        potential_mutes = [u for u in users if u["id"] != user["id"]]
        selected_mutes = random.sample(
            potential_mutes, min(num_mutes, len(potential_mutes))
        )

        for muted in selected_mutes:
            mutes.append(
                {
                    "id": str(uuid.uuid4()),
                    "user_id": user["id"],
                    "muted_id": muted["id"],
                    "muted_at": fake.date_between(
                        start_date="-1y", end_date="today"
                    ).strftime("%Y.%m.%d %H:%M"),
                    "duration_days": random.choice(
                        [7, 14, 30, 90, None]
                    ),  # None = бессрочно
                    "is_active": True,
                }
            )
    num_mutes = len(mutes)
    logging.info(f"Сгенерировано отключенных уведомлений: {len(mutes)}")
    logging.info(mutes)

    context["task_instance"].xcom_push(key="mutes", value=mutes)
    context["task_instance"].xcom_push(key="num_mutes", value=num_mutes)
    save_csv_file(temp_file_path_mutes, mutes)
    return mutes



def generate_close_friends(**context) -> List[Dict]:
    """Генерация близких друзей (close friends)"""
    friends = context["task_instance"].xcom_pull(key="friends", task_ids="generate_data_group.gen_friends")
    close_friends = []
    for friend in friends:
        if friend["is_best_friend"]:
            close_friends.append(
                {
                    "id": str(uuid.uuid4()),
                    "user_id": friend["user_id"],
                    "close_friend_id": friend["friend_id"],
                    "added_at": friend["created_at"],
                    "is_active": True,
                }
            )
    num_close_friends = len(close_friends)
    logging.info(f"Сгенерировано близких друзей: {len(close_friends)}")
    logging.info(close_friends)

    context["task_instance"].xcom_push(key="close_friends", value=close_friends)
    context["task_instance"].xcom_push(key="num_close_friends", value=num_close_friends)
    save_csv_file(temp_file_path_close_friends, close_friends)
    return close_friends



# # Генерация пользователей
# users = gen_user()

# # Генерация социальных связей
# friends = generate_friends(users)
# followers = generate_followers(users)
# subscriptions = generate_subscriptions(users)
# blocks = generate_blocks(users)
# mutes = generate_mutes(users)
# close_friends = generate_close_friends(friends)
