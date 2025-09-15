from confluent_kafka.admin import AdminClient, NewTopic
import logging


def create_kafka_topics(**context):
    """Создание топиков Kafka для медиа и контента"""
    try:
        conf = {"bootstrap.servers": "kafka:9092"}

        admin_client = AdminClient(conf)

        # Топики для медиа данных
        media_topics = [
            NewTopic(topic="media_photos", num_partitions=3, replication_factor=1),
            NewTopic(topic="media_videos", num_partitions=3, replication_factor=1),
            NewTopic(topic="media_albums", num_partitions=3, replication_factor=1),
        ]

        # Топики для контента
        content_topics = [
            NewTopic(topic="content_posts", num_partitions=3, replication_factor=1),
            NewTopic(topic="content_stories", num_partitions=3, replication_factor=1),
            NewTopic(topic="content_reels", num_partitions=3, replication_factor=1),
            NewTopic(topic="content_comments", num_partitions=3, replication_factor=1),
            NewTopic(topic="content_replies", num_partitions=3, replication_factor=1),
            NewTopic(topic="content_likes", num_partitions=3, replication_factor=1),
            NewTopic(topic="content_reactions", num_partitions=3, replication_factor=1),
            NewTopic(topic="content_shares", num_partitions=3, replication_factor=1),
        ]

        # Создание топиков
        all_topics = media_topics + content_topics
        fs = admin_client.create_topics(all_topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Топик {topic} успешно создан")
            except Exception as e:
                logging.error(f"Ошибка при создании топика {topic}: {str(e)}")
        logging.info("Топики Kafka успешно созданы")

    except Exception as e:
        logging.error(f"Ошибка при создании топиков Kafka: {str(e)}")
    
