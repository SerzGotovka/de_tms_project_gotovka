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
from utils.tg_bot import send_telegram_message


class DateTimeEncoder(json.JSONEncoder):
    """Кастомный JSON encoder для обработки datetime и date объектов"""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


class KafkaDataProducer:
    """Класс для отправки данных в Kafka топики"""
    
    def __init__(self, bootstrap_servers: str = "kafka:9092"):
        """Инициализация Kafka Producer"""
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'social_media_producer',
            'acks': 'all',  # Ждем подтверждения от всех реплик
            'retries': 3   # Количество повторных попыток
            
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
        """Отправка сообщения в Kafka топик"""
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
    
    def send_media_data(self, users: List[Dict[str, Any]], context: Dict[str, Any] = None):
        """Генерация и отправка медиа данных в соответствующие топики"""
        
        logging.info("Начинаем генерацию и отправку медиа данных в Kafka")
        
        # Генерация и отправка фото
        photos = generate_photos(num_photos=10, **context)
        for photo in photos:
            self.send_message(
                topic='photos',
                data=photo,
                key=photo.get('id', photo.get('user_id'))
            )
        
        # Генерация и отправка видео
        videos = generate_videos(num_videos=5, **context)
        for video in videos:
            self.send_message(
                topic='videos',
                data=video,
                key=video.get('id', video.get('user_id'))
            )
        
        # Генерация и отправка альбомов
        albums = generate_albums(num_albums=3, **context)
        for album in albums:
            self.send_message(
                topic='albums',
                data=album,
                key=album.get('id', album.get('user_id'))
            )
        
        # Ожидание доставки всех сообщений
        self.producer.flush(timeout=30)  # Ждем до 30 секунд
        logging.info("Отправка медиа данных завершена")
        
        # Отправка уведомления в Telegram с количеством загруженных файлов
        total_files = len(photos) + len(videos) + len(albums)
        message = f"📸 Медиа данные загружены в Kafka:\n"
        message += f"• Фото: {len(photos)}\n"
        message += f"• Видео: {len(videos)}\n"
        message += f"• Альбомы: {len(albums)}\n"
        message += f"• Всего файлов: {total_files}"
        
        try:
            if send_telegram_message(message):
                logging.info("Уведомление о загрузке медиа данных отправлено в Telegram")
            else:
                logging.error("Не удалось отправить уведомление о загрузке медиа данных в Telegram")
        except Exception as e:
            logging.error(f"Ошибка при отправке уведомления в Telegram: {str(e)}")
        
        return {
            'photos': photos,
            'videos': videos,
            'albums': albums
        }
    
    def send_content_data(self, users: List[Dict[str, Any]], context: Dict[str, Any] = None):
        """Генерация и отправка контент данных в соответствующие топики"""
        logging.info("Начинаем генерацию и отправку контент данных в Kafka")
        
        user_ids = [user['id'] for user in users]
        
        # Генерация и отправка постов
        posts = generate_posts(n=10, **context)
        for post in posts:
            self.send_message(
                topic='posts',
                data=post,
                key=str(post.get('id', post.get('author_id')))
            )
        
        # Генерация и отправка историй
        stories = generate_stories(n=5, **context)
        for story in stories:
            self.send_message(
                topic='stories',
                data=story,
                key=str(story.get('id', story.get('user_id')))
            )
        
        # Генерация и отправка Reels
        reels = generate_reels(n=5, **context)
        for reel in reels:
            self.send_message(
                topic='reels',
                data=reel,
                key=str(reel.get('id', reel.get('user_id')))
            )
        
        # Генерация и отправка комментариев
        comments = generate_comments(n=15, **context)
        for comment in comments:
            self.send_message(
                topic='comments',
                data=comment,
                key=str(comment.get('id', comment.get('post_id')))
            )
        
        # Генерация и отправка ответов на комментарии
        replies = generate_replies(n=10, **context)
        for reply in replies:
            self.send_message(
                topic='replies',
                data=reply,
                key=str(reply.get('id', reply.get('comment_id')))
            )
        
        # Генерация и отправка лайков
        likes = generate_likes(n=20, **context)
        for like in likes:
            self.send_message(
                topic='likes',
                data=like,
                key=str(like.get('id', like.get('post_id')))
            )
        
        # Генерация и отправка реакций
        reactions = generate_reactions(n=15, **context)
        for reaction in reactions:
            self.send_message(
                topic='reactions',
                data=reaction,
                key=str(reaction.get('id', reaction.get('post_id')))
            )
        
        # Генерация и отправка репостов
        shares = generate_shares(n=8, **context)
        for share in shares:
            self.send_message(
                topic='shares',
                data=share,
                key=str(share.get('id', share.get('post_id')))
            )
        
        # Ожидание доставки всех сообщений
        self.producer.flush(timeout=30)  # Ждем до 30 секунд
        logging.info("Отправка контент данных завершена")
        
        # Отправка уведомления в Telegram с количеством загруженных файлов
        total_files = len(posts) + len(stories) + len(reels) + len(comments) + len(replies) + len(likes) + len(reactions) + len(shares)
        message = f"📝 Контент данные загружены в Kafka:\n"
        message += f"• Посты: {len(posts)}\n"
        message += f"• Истории: {len(stories)}\n"
        message += f"• Reels: {len(reels)}\n"
        message += f"• Комментарии: {len(comments)}\n"
        message += f"• Ответы: {len(replies)}\n"
        message += f"• Лайки: {len(likes)}\n"
        message += f"• Реакции: {len(reactions)}\n"
        message += f"• Репосты: {len(shares)}\n"
        message += f"• Всего файлов: {total_files}"
        
        try:
            if send_telegram_message(message):
                logging.info("Уведомление о загрузке контент данных отправлено в Telegram")
            else:
                logging.error("Не удалось отправить уведомление о загрузке контент данных в Telegram")
        except Exception as e:
            logging.error(f"Ошибка при отправке уведомления в Telegram: {str(e)}")
        
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
        media_data = producer.send_media_data(users, context)
        producer.close()
        
        logging.info(f"Медиа данные успешно отправлены в Kafka: {len(media_data['photos'])} фото, {len(media_data['videos'])} видео, {len(media_data['albums'])} альбомов")
        
        # Отправка уведомления в Telegram для отдельной функции
        total_files = len(media_data['photos']) + len(media_data['videos']) + len(media_data['albums'])
        message = f"📸 Медиа данные загружены в Kafka (отдельная задача):\n"
        message += f"• Фото: {len(media_data['photos'])}\n"
        message += f"• Видео: {len(media_data['videos'])}\n"
        message += f"• Альбомы: {len(media_data['albums'])}\n"
        message += f"• Всего файлов: {total_files}"
        
        try:
            if send_telegram_message(message):
                logging.info("Уведомление о загрузке медиа данных (отдельная задача) отправлено в Telegram")
            else:
                logging.error("Не удалось отправить уведомление о загрузке медиа данных (отдельная задача) в Telegram")
        except Exception as e:
            logging.error(f"Ошибка при отправке уведомления в Telegram: {str(e)}")
        
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
        content_data = producer.send_content_data(users, context)
        producer.close()
        
        logging.info(f"Контент данные успешно отправлены в Kafka: {len(content_data['posts'])} постов, {len(content_data['stories'])} историй, {len(content_data['reels'])} reels")
        
        # Отправка уведомления в Telegram для отдельной функции
        total_files = len(content_data['posts']) + len(content_data['stories']) + len(content_data['reels']) + len(content_data['comments']) + len(content_data['replies']) + len(content_data['likes']) + len(content_data['reactions']) + len(content_data['shares'])
        message = f"📝 Контент данные загружены в Kafka (отдельная задача):\n"
        message += f"• Посты: {len(content_data['posts'])}\n"
        message += f"• Истории: {len(content_data['stories'])}\n"
        message += f"• Reels: {len(content_data['reels'])}\n"
        message += f"• Комментарии: {len(content_data['comments'])}\n"
        message += f"• Ответы: {len(content_data['replies'])}\n"
        message += f"• Лайки: {len(content_data['likes'])}\n"
        message += f"• Реакции: {len(content_data['reactions'])}\n"
        message += f"• Репосты: {len(content_data['shares'])}\n"
        message += f"• Всего файлов: {total_files}"
        
        try:
            if send_telegram_message(message):
                logging.info("Уведомление о загрузке контент данных (отдельная задача) отправлено в Telegram")
            else:
                logging.error("Не удалось отправить уведомление о загрузке контент данных (отдельная задача) в Telegram")
        except Exception as e:
            logging.error(f"Ошибка при отправке уведомления в Telegram: {str(e)}")
        
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
        media_data = producer.send_media_data(users, context)
        content_data = producer.send_content_data(users, context)
        producer.close()
        
        # Отправка общего уведомления в Telegram с общим количеством загруженных файлов
        total_media_files = len(media_data['photos']) + len(media_data['videos']) + len(media_data['albums'])
        total_content_files = len(content_data['posts']) + len(content_data['stories']) + len(content_data['reels']) + len(content_data['comments']) + len(content_data['replies']) + len(content_data['likes']) + len(content_data['reactions']) + len(content_data['shares'])
        total_files = total_media_files + total_content_files
        
        message = f"🚀 Все данные успешно загружены в Kafka:\n"
        message += f"📸 Медиа файлы: {total_media_files}\n"
        message += f"📝 Контент файлы: {total_content_files}\n"
        message += f"📊 Всего файлов: {total_files}"
        
        try:
            if send_telegram_message(message):
                logging.info("Общее уведомление о загрузке всех данных отправлено в Telegram")
            else:
                logging.error("Не удалось отправить общее уведомление о загрузке всех данных в Telegram")
        except Exception as e:
            logging.error(f"Ошибка при отправке общего уведомления в Telegram: {str(e)}")
        
        logging.info("Все данные успешно отправлены в Kafka")
        return {
            'media_data': media_data,
            'content_data': content_data
        }
        
    except Exception as e:
        logging.error(f"Ошибка при отправке данных в Kafka: {str(e)}")
        raise


