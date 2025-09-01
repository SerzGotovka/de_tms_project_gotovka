from confluent_kafka.admin import AdminClient, NewTopic
import logging


def create_kafka_topics(**context):
    """Создание топиков Kafka для медиа и контента"""
    try:
        conf = {"bootstrap.servers": "kafka:9092"}

        admin_client = AdminClient(conf)

        # Топики для медиа данных
        media_topics = [
            NewTopic(topic="photos", num_partitions=3, replication_factor=1),
            NewTopic(topic="videos", num_partitions=3, replication_factor=1),
            NewTopic(topic="albums", num_partitions=3, replication_factor=1),
        ]

        # Топики для контента
        content_topics = [
            NewTopic(topic="posts", num_partitions=3, replication_factor=1),
            NewTopic(topic="stories", num_partitions=3, replication_factor=1),
            NewTopic(topic="reels", num_partitions=3, replication_factor=1),
            NewTopic(topic="comments", num_partitions=3, replication_factor=1),
            NewTopic(topic="replies", num_partitions=3, replication_factor=1),
            NewTopic(topic="likes", num_partitions=3, replication_factor=1),
            NewTopic(topic="reactions", num_partitions=3, replication_factor=1),
            NewTopic(topic="shares", num_partitions=3, replication_factor=1),
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
    
