from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
from typing import List, Dict
from generate_users import gen_user
from config.config_generate import NUM_COMMUNITIES, NUM_GROUPS, MAX_MEMBERS_PER_GROUP
import logging

fake = Faker()



def generate_communities(n=NUM_COMMUNITIES) -> List[Dict]:
    """Генерация сообществ"""
    communities = []
    for i in range(n):
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
    logging.info(f"Сгенерировано сообществ: {len(communities)}")
    logging.info(communities)
    return communities



def generate_groups(user_ids: List[str], n=NUM_GROUPS) -> List[Dict]:
    """Генерация групп с владельцами"""
    groups = []
    for i in range(n):
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
    logging.info(f"Сгенерировано групп: {len(groups)}")
    logging.info(groups)
    return groups



def generate_group_members(group_ids: List[str], user_ids: List[str], n=MAX_MEMBERS_PER_GROUP) -> List[Dict]:
    """Генерация участников групп"""
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
    logging.info(f"Сгенерировано участников групп: {len(members)}")
    logging.info(members)
    return members



def generate_community_topics(communities: List[Dict], n=3) -> List[Dict]:
    """Генерация тем для сообществ"""
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
    logging.info(f"Сгенерировано тем для сообществ: {len(topics)}")
    logging.info(topics)
    return topics



def generate_pinned_posts(
    communities: List[Dict], groups: List[Dict], user_ids: List[str], n=5) -> List[Dict]:
    """Генерация закрепленных постов"""
    pinned_posts = []
    community_ids = [c["id"] for c in communities]
    group_ids = [g["id"] for g in groups]

    for i in range(n):
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
    logging.info(f"Сгенерировано закрепленных постов: {len(pinned_posts)}")
    logging.info(pinned_posts)
    return pinned_posts



def generate_all_group_data():
    user_ids = gen_user()

    # Генерация сообществ
    communities = generate_communities()
    community_ids = [c["id"] for c in communities]

    # Генерация групп
    groups = generate_groups(user_ids)
    group_ids = [g["id"] for g in groups]

    # Генерация участников
    group_members = generate_group_members(group_ids, user_ids)

    # Генерация тем
    community_topics = generate_community_topics(communities)

    # Генерация закрепленных постов
    pinned_posts = generate_pinned_posts(communities, groups, user_ids)

    return {
        "communities": communities,
        "community_topics": community_topics,
        "groups": groups,
        "group_members": group_members,
        "pinned_posts": pinned_posts,
    }
