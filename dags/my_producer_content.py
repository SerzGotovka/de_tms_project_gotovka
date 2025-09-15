from confluent_kafka.admin import AdminClient, NewTopic
import json
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
import logging
from generate_data.generate_content import (
    generate_posts, generate_stories, generate_reels, 
    generate_comments, generate_replies, generate_likes, 
    generate_reactions, generate_shares
)
from generate_data.generate_users import gen_user


def my_producer_content(content_type: str, **context):
    """Функция для отправки контента (посты, истории, reels и т.д.) в Kafka"""
    try:
        # Конфигурация для Kafka
        conf = {
            'bootstrap.servers': 'kafka:9092'
        }

        admin = AdminClient(conf)

        # Создание нового топика
        new_topic = NewTopic(
            topic=f'content_{content_type}',
            num_partitions=3,
            replication_factor=1
        )

        fs = admin.create_topics([new_topic])

        for topic, f in fs.items():
            try:
                f.result()
                print(f"[v] Топик {topic} создан")
            except Exception as e:
                print(f"Ошибка при создании топика {topic}: {str(e)}")

        producer = Producer(conf)
        TOPIC = f'content_{content_type}'

        # Генерируем данные самостоятельно, так как XCom недоступен между DAG'ами
        print(f"🔄 Генерируем {content_type} данные...")
        
        # Генерируем контент в зависимости от типа
        if content_type == 'posts':
            data = generate_posts(n=10, **context)
        elif content_type == 'stories':
            data = generate_stories(n=8, **context)
        elif content_type == 'reels':
            data = generate_reels(n=5, **context)
        elif content_type == 'comments':
            data = generate_comments(n=15, **context)
        elif content_type == 'replies':
            data = generate_replies(n=10, **context)
        elif content_type == 'likes':
            data = generate_likes(n=20, **context)
        elif content_type == 'reactions':
            data = generate_reactions(n=15, **context)
        elif content_type == 'shares':
            data = generate_shares(n=10, **context)
        else:
            print(f"❌ Неподдерживаемый тип контента: {content_type}")
            return []

        if not data:
            print(f"❌ Не удалось сгенерировать данные для {content_type}")
            return []
        
        print(f"✅ Сгенерировано {len(data)} {content_type} для отправки")
        
        # Отправляем данные в Kafka
        for item in data:
            try:
                # Убеждаемся, что данные сериализуются в JSON
                json_data = json.dumps(item, ensure_ascii=False, default=str)
                producer.produce(TOPIC, json_data.encode('utf-8'))
            except Exception as e:
                print(f"Ошибка при отправке элемента {item.get('id', 'unknown')}: {e}")
                continue
                
        producer.flush()
        print(f"✅ Отправлено {len(data)} {content_type} в топик {TOPIC}")
        return data
        
    except Exception as e:
        print(f"❌ Критическая ошибка в my_producer_content: {e}")
        logging.error(f"Ошибка в my_producer_content для {content_type}: {e}")
        raise
    finally:
        try:
            producer.close()
        except:
            pass


def my_producer_posts(**context):
    """Функция для отправки постов в Kafka"""
    return my_producer_content('posts', **context)


def my_producer_stories(**context):
    """Функция для отправки историй в Kafka"""
    return my_producer_content('stories', **context)


def my_producer_reels(**context):
    """Функция для отправки reels в Kafka"""
    return my_producer_content('reels', **context)


def my_producer_comments(**context):
    """Функция для отправки комментариев в Kafka"""
    return my_producer_content('comments', **context)


def my_producer_replies(**context):
    """Функция для отправки ответов на комментарии в Kafka"""
    return my_producer_content('replies', **context)


def my_producer_likes(**context):
    """Функция для отправки лайков в Kafka"""
    return my_producer_content('likes', **context)


def my_producer_reactions(**context):
    """Функция для отправки реакций в Kafka"""
    return my_producer_content('reactions', **context)


def my_producer_shares(**context):
    """Функция для отправки репостов в Kafka"""
    return my_producer_content('shares', **context)
