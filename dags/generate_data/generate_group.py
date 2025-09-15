from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
from typing import List, Dict
from utils.config_generate import NUM_COMMUNITIES, NUM_GROUPS, MAX_MEMBERS_PER_GROUP
import logging
from utils.function_minio import save_csv_file, save_data_directly_to_minio
from utils.config_generate import (temp_file_path_communities, temp_file_path_community_topics,
                                   temp_file_path_group_members, temp_file_path_pinned_posts, temp_file_path_groups)

fake = Faker()


    
def generate_communities(n=NUM_COMMUNITIES, **context) -> List[Dict]:
    """Генерация сообществ"""
    communities = []
    for _ in range(n):
        record = {
            "id": str(uuid.uuid4()),
            "name": fake.bs().title(),
            "description": fake.paragraph(),
            "created_at": fake.date_between(
                start_date="-1y", end_date="today"
            ).strftime("%Y.%m.%d %H:%M"),
            "member_count": random.randint(100, 10000),
        }
        communities.append(record)
    num_communities = len(communities)
    logging.info(f"Сгенерировано сообществ: {len(communities)}")
    logging.info(communities)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_communities_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=communities,
            filename=filename,
            folder="/groups_communities/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} сообществ в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи сообществ в MinIO: {e}")
        raise

    # communities = generate_communities()

    context["task_instance"].xcom_push(key="communities", value=communities)
    context["task_instance"].xcom_push(key="num_communities", value=num_communities)
    return communities



def generate_groups(n=NUM_GROUPS, **context) -> List[Dict]:
    """Генерация групп с владельцами"""
    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    user_ids = [u["id"] for u in users]
    groups = []
    for _ in range(n):
        record = {
            "id": str(uuid.uuid4()),
            "owner_id": random.choice(user_ids),
            "name": fake.catch_phrase(),
            "description": fake.text(max_nb_chars=200),
            "privacy": random.choice(["public", "private", "hidden"]),
            "created_at": fake.date_between(
                start_date="-1y", end_date="today"
            ).strftime("%Y.%m.%d %H:%M"),
        }
        groups.append(record)
    num_groups = len(groups)
    logging.info(f"Сгенерировано групп: {len(groups)}")
    logging.info(groups)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_groups_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=groups,
            filename=filename,
            folder="/groups/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} групп в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи групп в MinIO: {e}")
        raise
    
    # groups = generate_groups(user_ids)

    context["task_instance"].xcom_push(key="groups", value=groups)
    context["task_instance"].xcom_push(key="num_groups", value=num_groups)
    
    return groups


def generate_group_members(n=MAX_MEMBERS_PER_GROUP, **context) -> List[Dict]:
    """Генерация участников групп"""

    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    groups = context["task_instance"].xcom_pull(key="groups", task_ids="generate_data_group.gen_groups")
    user_ids = [u["id"] for u in users]
    group_ids = [g["id"] for g in groups]

    members = []
    for group_id in group_ids:
        num_members = random.randint(3, n)
        group_users = random.sample(user_ids, num_members)

        for user_id in group_users:
            members.append(
                {
                    "id": str(uuid.uuid4()),
                    "group_id": group_id,
                    "user_id": user_id,
                    "joined_at": fake.date_between(
                        start_date="-1y", end_date="today"
                    ).strftime("%Y.%m.%d %H:%M"),
                    "role": random.choice(["member", "moderator", "admin"]),
                }
            )
    num_group_members = len(members)
    logging.info(f"Сгенерировано участников групп: {len(members)}")
    logging.info(members)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_group_members_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=members,
            filename=filename,
            folder="/group_members/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} участников групп в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи участников групп в MinIO: {e}")
        raise
    
    # members = generate_group_members(group_ids, user_ids)

    context["task_instance"].xcom_push(key="group_members", value=members)
    context["task_instance"].xcom_push(key="num_group_members", value=num_group_members)
    return members



def generate_community_topics(n=3, **context) -> List[Dict]:
    """Генерация тем для сообществ"""

    communities = context["task_instance"].xcom_pull(
        key="communities", task_ids="generate_data_group.gen_communities"
    )
    topics = []
    for community in communities:
        for i in range(n):
            topics.append(
                {
                    "id": str(uuid.uuid4()),
                    "community_id": community["id"],
                    "title": fake.sentence(nb_words=6),
                    "description": fake.sentence(nb_words=10),
                    "created_at": fake.date_between(
                        start_date="-1y", end_date="today"
                    ).strftime("%Y.%m.%d %H:%M"),
                }
            )
    num_community_topics = len(topics)
    logging.info(f"Сгенерировано тем для сообществ: {len(topics)}")
    logging.info(topics)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_community_topics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=topics,
            filename=filename,
            folder="/community_topics/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} тем сообществ в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи тем сообществ в MinIO: {e}")
        raise
    
    # topics = generate_community_topics(communities)
    context["task_instance"].xcom_push(key="num_community_topics", value=num_community_topics)
    context["task_instance"].xcom_push(key="community_topics", value=topics)
    
    return topics



def generate_pinned_posts(n=5, **context) -> List[Dict]:
    """Генерация закрепленных постов"""

    users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
    communities = context["task_instance"].xcom_pull(
        key="communities", task_ids="generate_data_group.gen_communities"
    )
    groups = context["task_instance"].xcom_pull(key="groups", task_ids="generate_data_group.gen_groups")
    user_ids = [u["id"] for u in users]
    # pinned_posts = generate_pinned_posts(communities, groups, user_ids, n=5)

 
    pinned_posts = []
    community_ids = [c["id"] for c in communities]
    group_ids = [g["id"] for g in groups]

    for _ in range(n):
        source_type = random.choice(["community", "group"])

        if source_type == "community":
            source_id = random.choice(community_ids)
            group_id = None
        else:
            source_id = None
            group_id = random.choice(group_ids)

        pinned_posts.append(
            {
                "id": str(uuid.uuid4()),
                "community_id": source_id,
                "group_id": group_id,
                "author_id": random.choice(user_ids),
                "content": fake.paragraph(),
                "pinned_at": fake.date_between(
                    start_date="-1y", end_date="today"
                ).strftime("%Y.%m.%d %H:%M"),
                "expires_at": (
                    datetime.now() + timedelta(days=random.randint(1, 30))
                ).strftime("%Y.%m.%d %H:%M"),
            }
        )
    num_pinned_posts = len(pinned_posts)
    logging.info(f"Сгенерировано закрепленных постов: {len(pinned_posts)}")
    logging.info(pinned_posts)

    # Записываем данные напрямую в MinIO
    try:
        filename = f'data_pinned_posts_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        record_count = save_data_directly_to_minio(
            data=pinned_posts,
            filename=filename,
            folder="/pinned_posts/",
            bucket_name="data-bucket"
        )
        logging.info(f"✅ Записано {record_count} закрепленных постов в MinIO")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи закрепленных постов в MinIO: {e}")
        raise
    
    context["task_instance"].xcom_push(key="pinned_posts", value=pinned_posts)
    context["task_instance"].xcom_push(key="num_pinned_posts", value=num_pinned_posts)
    return pinned_posts



# def generate_all_group_data(users: List[Dict]):
#     user_ids = [user['id'] for user in users]

#     # Генерация сообществ
#     communities = generate_communities()
#     community_ids = [c["id"] for c in communities]

#     # Генерация групп
#     groups = generate_groups(user_ids)
#     group_ids = [g["id"] for g in groups]

#     # Генерация участников
#     group_members = generate_group_members(group_ids, user_ids)

#     # Генерация тем
#     community_topics = generate_community_topics(communities)

#     # Генерация закрепленных постов
#     pinned_posts = generate_pinned_posts(communities, groups, user_ids)

#     return {
#         "communities": communities,
#         "community_topics": community_topics,
#         "groups": groups,
#         "group_members": group_members,
#         "pinned_posts": pinned_posts,
#     }
