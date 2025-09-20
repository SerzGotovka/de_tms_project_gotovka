from faker import Faker
import random
import uuid
from typing import List, Dict
import logging
from utils.function_minio import save_csv_file, save_data_directly_to_minio
from utils.config_generate import (temp_file_path_friends, temp_file_path_followers, temp_file_path_subscriptions,
                                   temp_file_path_blocks, temp_file_path_mutes, temp_file_path_close_friends, temp_file_path_social_groups)
from datetime import datetime

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

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_friends_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=friends,
            filename=filename,
            folder="social_friends/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} дружеских связей в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи дружеских связей в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="friends", value=friends)
    context["task_instance"].xcom_push(key="num_friends", value=num_friends)
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

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_followers_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=followers,
            filename=filename,
            folder="social_followers/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} подписчиков в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи подписчиков в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="followers", value=followers)
    context["task_instance"].xcom_push(key="num_followers", value=num_followers)
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

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_subscriptions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=subscriptions,
            filename=filename,
            folder="social_subscriptions/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} подписок в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи подписок в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="subscriptions", value=subscriptions)
    context["task_instance"].xcom_push(key="num_subscriptions", value=num_subscriptions)
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

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_blocks_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=blocks,
            filename=filename,
            folder="social_blocks/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} блокировок в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи блокировок в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="blocks", value=blocks)
    context["task_instance"].xcom_push(key="num_blocks", value=num_blocks)
    
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

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_mutes_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=mutes,
            filename=filename,
            folder="social_mutes/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} отключенных уведомлений в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи отключенных уведомлений в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="mutes", value=mutes)
    context["task_instance"].xcom_push(key="num_mutes", value=num_mutes)
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

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_close_friends_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=close_friends,
            filename=filename,
            folder="social_close_friends/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} близких друзей в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи близких друзей в MinIO: {e}")
        raise

    context["task_instance"].xcom_push(key="close_friends", value=close_friends)
    context["task_instance"].xcom_push(key="num_close_friends", value=num_close_friends)
    return close_friends


def generate_social_data(**context) -> List[Dict]:
    """Генерация общего файла социальных данных"""
    from datetime import datetime
    
    # Получаем все социальные данные из XCom
    friends = context["task_instance"].xcom_pull(key="friends", task_ids="generate_data_group.gen_friends")
    followers = context["task_instance"].xcom_pull(key="followers", task_ids="generate_data_group.gen_followers")
    subscriptions = context["task_instance"].xcom_pull(key="subscriptions", task_ids="generate_data_group.gen_subscriptions")
    blocks = context["task_instance"].xcom_pull(key="blocks", task_ids="generate_data_group.gen_blocks")
    mutes = context["task_instance"].xcom_pull(key="mutes", task_ids="generate_data_group.gen_mutes")
    close_friends = context["task_instance"].xcom_pull(key="close_friends", task_ids="generate_data_group.gen_close_friends")
    
    # Объединяем все социальные данные
    social_data = []
    
    # Добавляем друзей
    if friends:
        for friend in friends:
            social_data.append({
                "id": friend["id"],
                "user_id": friend["user_id"],
                "target_id": friend["friend_id"],
                "relationship_type": "friend",
                "created_at": friend["created_at"],
                "status": friend["status"],
                "is_best_friend": friend["is_best_friend"]
            })
    
    # Добавляем подписчиков
    if followers:
        for follower in followers:
            social_data.append({
                "id": follower["id"],
                "user_id": follower["user_id"],
                "target_id": follower["follower_id"],
                "relationship_type": "follower",
                "created_at": follower["followed_at"],
                "status": "active" if follower["is_active"] else "inactive",
                "is_best_friend": False
            })
    
    # Добавляем подписки
    if subscriptions:
        for subscription in subscriptions:
            social_data.append({
                "id": subscription["id"],
                "user_id": subscription["user_id"],
                "target_id": subscription["subscribed_to"],
                "relationship_type": "subscription",
                "created_at": subscription["subscribed_at"],
                "status": "active" if subscription["is_active"] else "inactive",
                "is_best_friend": False
            })
    
    # Добавляем блокировки
    if blocks:
        for block in blocks:
            social_data.append({
                "id": block["id"],
                "user_id": block["user_id"],
                "target_id": block["blocked_id"],
                "relationship_type": "block",
                "created_at": block["blocked_at"],
                "status": "active" if block["is_active"] else "inactive",
                "is_best_friend": False
            })
    
    # Добавляем отключенные уведомления
    if mutes:
        for mute in mutes:
            social_data.append({
                "id": mute["id"],
                "user_id": mute["user_id"],
                "target_id": mute["muted_id"],
                "relationship_type": "mute",
                "created_at": mute["muted_at"],
                "status": "active" if mute["is_active"] else "inactive",
                "is_best_friend": False
            })
    
    # Добавляем близких друзей
    if close_friends:
        for close_friend in close_friends:
            social_data.append({
                "id": close_friend["id"],
                "user_id": close_friend["user_id"],
                "target_id": close_friend["close_friend_id"],
                "relationship_type": "close_friend",
                "created_at": close_friend["added_at"],
                "status": "active" if close_friend["is_active"] else "inactive",
                "is_best_friend": True
            })
    
    num_social = len(social_data)
    logging.info(f"Сгенерировано социальных связей: {len(social_data)}")
    
    context["task_instance"].xcom_push(key="social_data", value=social_data)
    context["task_instance"].xcom_push(key="num_social", value=num_social)
    
    return social_data



# # Генерация пользователей
# users = gen_user()

# # Генерация социальных связей
# friends = generate_friends(users)
# followers = generate_followers(users)
# subscriptions = generate_subscriptions(users)
# blocks = generate_blocks(users)
# mutes = generate_mutes(users)
# close_friends = generate_close_friends(friends)
