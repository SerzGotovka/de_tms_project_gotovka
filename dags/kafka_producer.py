import json
import logging
from confluent_kafka import Producer
from typing import List, Dict, Any
from datetime import datetime, date
from generate_data.generate_media import generate_photos, generate_videos, generate_albums
from generate_data.generate_content import (
    generate_posts, generate_stories, generate_reels,
    generate_comments, generate_replies, generate_likes,
    generate_reactions, generate_shares
)


class DateTimeEncoder(json.JSONEncoder):
    """Кастомный JSON encoder для обработки datetime и date объектов"""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


class KafkaDataProducer:
    """Класс для отправки данных в Kafka топики"""
    
    def __init__(self, bootstrap_servers: str = "kafka:9092"):
        """
        Инициализация Kafka Producer
        
        Args:
            bootstrap_servers: Адрес Kafka сервера
        """
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'social_media_producer',
            'acks': 'all',  # Ждем подтверждения от всех реплик
            'retries': 3,   # Количество повторных попыток
            'batch.size': 16384,  # Размер батча
            'linger.ms': 10  # Время ожидания для батча
            
        }
        
        try:
            self.producer = Producer(self.conf)
            logging.info(f"Kafka Producer инициализирован с сервером: {bootstrap_servers}")
        except Exception as e:
            logging.error(f"Ошибка при инициализации Kafka Producer: {str(e)}")
            raise
    
    def delivery_report(self, err, msg):
        """Callback функция для обработки результатов доставки сообщений"""
        if err is not None:
            logging.error(f'Ошибка доставки сообщения: {err}')
            if hasattr(err, 'code'):
                logging.error(f'Код ошибки: {err.code()}')
        else:
            logging.info(f'Сообщение успешно доставлено в топик {msg.topic()} [раздел {msg.partition()}] на смещение {msg.offset()}')
            logging.info(f'Размер сообщения: {len(msg.value())} байт')
    
    def send_message(self, topic: str, data: Dict[str, Any], key: str = None):
        """
        Отправка сообщения в Kafka топик
        
        Args:
            topic: Название топика
            data: Данные для отправки
            key: Ключ сообщения (опционально)
        """
        try:
            # Сериализация данных в JSON с поддержкой datetime/date
            message = json.dumps(data, ensure_ascii=False, cls=DateTimeEncoder)
            
            logging.info(f"Отправка сообщения в топик {topic}: {message[:100]}...")
            
            # Отправка сообщения
            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8') if key else None,
                value=message.encode('utf-8'),
                callback=self.delivery_report
            )
            
            # Обработка очереди сообщений
            self.producer.poll(0)
            
            logging.info(f"Сообщение поставлено в очередь для топика {topic}")
            
        except Exception as e:
            logging.error(f"Ошибка при отправке сообщения в топик {topic}: {str(e)}")
            raise
    
    def send_media_data(self, users: List[Dict[str, Any]]):
        """
        Генерация и отправка медиа данных в соответствующие топики
        
        Args:
            users: Список пользователей для генерации данных
        """
        logging.info("Начинаем генерацию и отправку медиа данных в Kafka")
        
        # Генерация и отправка фото
        photos = generate_photos(users, num_photos=10)
        for photo in photos:
            self.send_message(
                topic='photos',
                data=photo,
                key=photo.get('id', photo.get('user_id'))
            )
        
        # Генерация и отправка видео
        videos = generate_videos(users, num_videos=5)
        for video in videos:
            self.send_message(
                topic='videos',
                data=video,
                key=video.get('id', video.get('user_id'))
            )
        
        # Генерация и отправка альбомов
        albums = generate_albums(users, photos, videos, num_albums=3)
        for album in albums:
            self.send_message(
                topic='albums',
                data=album,
                key=album.get('id', album.get('user_id'))
            )
        
        # Ожидание доставки всех сообщений
        self.producer.flush(timeout=30)  # Ждем до 30 секунд
        logging.info("Отправка медиа данных завершена")
        
        return {
            'photos': photos,
            'videos': videos,
            'albums': albums
        }
    
    def send_content_data(self, users: List[Dict[str, Any]]):
        """
        Генерация и отправка контент данных в соответствующие топики
        
        Args:
            users: Список пользователей для генерации данных
        """
        logging.info("Начинаем генерацию и отправку контент данных в Kafka")
        
        user_ids = [user['id'] for user in users]
        
        # Генерация и отправка постов
        posts = generate_posts(user_ids, n=10)
        for post in posts:
            self.send_message(
                topic='posts',
                data=post,
                key=str(post.get('id', post.get('author_id')))
            )
        
        # Генерация и отправка историй
        stories = generate_stories(user_ids, n=5)
        for story in stories:
            self.send_message(
                topic='stories',
                data=story,
                key=str(story.get('id', story.get('user_id')))
            )
        
        # Генерация и отправка Reels
        reels = generate_reels(user_ids, n=5)
        for reel in reels:
            self.send_message(
                topic='reels',
                data=reel,
                key=str(reel.get('id', reel.get('user_id')))
            )
        
        # Генерация и отправка комментариев
        comments = generate_comments(user_ids, posts, n=15)
        for comment in comments:
            self.send_message(
                topic='comments',
                data=comment,
                key=str(comment.get('id', comment.get('post_id')))
            )
        
        # Генерация и отправка ответов на комментарии
        replies = generate_replies(user_ids, comments, n=10)
        for reply in replies:
            self.send_message(
                topic='replies',
                data=reply,
                key=str(reply.get('id', reply.get('comment_id')))
            )
        
        # Генерация и отправка лайков
        likes = generate_likes(user_ids, posts, n=20)
        for like in likes:
            self.send_message(
                topic='likes',
                data=like,
                key=str(like.get('id', like.get('post_id')))
            )
        
        # Генерация и отправка реакций
        reactions = generate_reactions(user_ids, posts, n=15)
        for reaction in reactions:
            self.send_message(
                topic='reactions',
                data=reaction,
                key=str(reaction.get('id', reaction.get('post_id')))
            )
        
        # Генерация и отправка репостов
        shares = generate_shares(user_ids, posts, n=8)
        for share in shares:
            self.send_message(
                topic='shares',
                data=share,
                key=str(share.get('id', share.get('post_id')))
            )
        
        # Ожидание доставки всех сообщений
        self.producer.flush(timeout=30)  # Ждем до 30 секунд
        logging.info("Отправка контент данных завершена")
        
        return {
            'posts': posts,
            'stories': stories,
            'reels': reels,
            'comments': comments,
            'replies': replies,
            'likes': likes,
            'reactions': reactions,
            'shares': shares
        }
    
    def close(self):
        """Закрытие соединения с Kafka"""
        try:
            self.producer.flush(timeout=10)
            logging.info("Kafka Producer соединение закрыто")
        except Exception as e:
            logging.error(f"Ошибка при закрытии соединения: {str(e)}")


def send_media_to_kafka(**context):
    """Функция для отправки медиа данных в Kafka"""
    try:
        # Получаем данные пользователей из XCom
        users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        if users is None:
            users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
        
        if users is None:
            raise ValueError("Не удалось получить данные пользователей из XCom")
        
        # Создаем producer и отправляем данные
        producer = KafkaDataProducer()
        media_data = producer.send_media_data(users)
        producer.close()
        
        logging.info(f"Медиа данные успешно отправлены в Kafka: {len(media_data['photos'])} фото, {len(media_data['videos'])} видео, {len(media_data['albums'])} альбомов")
        return media_data
        
    except Exception as e:
        logging.error(f"Ошибка при отправке медиа данных в Kafka: {str(e)}")
        raise


def send_content_to_kafka(**context):
    """Функция для отправки контент данных в Kafka"""
    try:
        # Получаем данные пользователей из XCom
        users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        if users is None:
            users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
        
        if users is None:
            raise ValueError("Не удалось получить данные пользователей из XCom")
        
        # Создаем producer и отправляем данные
        producer = KafkaDataProducer()
        content_data = producer.send_content_data(users)
        producer.close()
        
        logging.info(f"Контент данные успешно отправлены в Kafka: {len(content_data['posts'])} постов, {len(content_data['stories'])} историй, {len(content_data['reels'])} reels")
        return content_data
        
    except Exception as e:
        logging.error(f"Ошибка при отправке контент данных в Kafka: {str(e)}")
        raise


def send_all_to_kafka(**context):
    """Функция для отправки всех данных в Kafka"""
    try:
        # Получаем данные пользователей из XCom
        users = context["task_instance"].xcom_pull(key="users", task_ids="gen_users")
        if users is None:
            users = context["task_instance"].xcom_pull(key="users", task_ids="generate_data_group.gen_users")
        
        if users is None:
            raise ValueError("Не удалось получить данные пользователей из XCom")
        
        # Создаем producer и отправляем все данные
        producer = KafkaDataProducer()
        media_data = producer.send_media_data(users)
        content_data = producer.send_content_data(users)
        producer.close()
        
        logging.info("Все данные успешно отправлены в Kafka")
        return {
            'media_data': media_data,
            'content_data': content_data
        }
        
    except Exception as e:
        logging.error(f"Ошибка при отправке данных в Kafka: {str(e)}")
        raise


def test_kafka_connection(bootstrap_servers: str = "kafka:9092"):
    """Тестирование подключения к Kafka"""
    try:
        logging.info(f"Тестирование подключения к Kafka: {bootstrap_servers}")
        
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'test_producer'
        }
        
        producer = Producer(conf)
        
        # Отправляем тестовое сообщение
        test_data = {
            "test": True,
            "timestamp": datetime.now().isoformat(),
            "message": "Тестовое сообщение для проверки подключения"
        }
        
        producer.produce(
            topic='test_topic',
            value=json.dumps(test_data, cls=DateTimeEncoder).encode('utf-8'),
            callback=lambda err, msg: logging.info(f"Тестовое сообщение доставлено: {msg.topic()}") if not err else logging.error(f"Ошибка доставки: {err}")
        )
        
        producer.poll(10)  # Ждем 10 секунд
        producer.flush(timeout=10)
        
        logging.info("Подключение к Kafka успешно протестировано")
        return True
        
    except Exception as e:
        logging.error(f"Ошибка подключения к Kafka: {str(e)}")
        return False