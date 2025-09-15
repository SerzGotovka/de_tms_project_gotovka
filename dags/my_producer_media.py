from confluent_kafka.admin import AdminClient, NewTopic
import json
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
import logging


def my_producer_media(media_type: str, **context):
    """Функция для отправки медиа (фото или видео) в Kafka"""
    try:
        # Конфигурация для Kafka
        conf = {
            'bootstrap.servers': 'kafka:9092'
        }

        admin = AdminClient(conf)

        # Создание нового топика
        new_topic = NewTopic(
            topic=f'media_{media_type}',
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
        TOPIC = f'media_{media_type}'

        # Получаем данные из XCom
        print(f"🔄 Получаем {media_type} данные из XCom...")
        
        # Получаем пользователей из XCom
        users = context['task_instance'].xcom_pull(key='users', task_ids='generate_data_group.gen_users')
        print(f"✅ Получено {len(users)} пользователей из XCom")
        
        if media_type == 'photos':
            data = context['task_instance'].xcom_pull(key='photos', task_ids='generate_data_group.gen_photos')
        elif media_type == 'videos':
            data = context['task_instance'].xcom_pull(key='videos', task_ids='generate_data_group.gen_videos')
        elif media_type == 'albums':
            # Для альбомов получаем фото, видео и альбомы из XCom
            photos = context['task_instance'].xcom_pull(key='photos', task_ids='generate_data_group.gen_photos')
            videos = context['task_instance'].xcom_pull(key='videos', task_ids='generate_data_group.gen_videos')
            data = context['task_instance'].xcom_pull(key='albums', task_ids='generate_data_group.gen_albums')
        else:
            print(f"❌ Неподдерживаемый тип медиа: {media_type}")
            return []

        if not data:
            print(f"❌ Не удалось сгенерировать данные для {media_type}")
            return []
        
        print(f"✅ Сгенерировано {len(data)} {media_type} для отправки")
        
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
        print(f"✅ Отправлено {len(data)} {media_type} в топик {TOPIC}")
        return data
        
    except Exception as e:
        print(f"❌ Критическая ошибка в my_producer_media: {e}")
        logging.error(f"Ошибка в my_producer_media для {media_type}: {e}")
        raise
    finally:
        try:
            producer.close()
        except:
            pass


def my_producer_albums(**context):
    """Функция для отправки данных альбомов в Kafka"""
    try:
        # Конфигурация для Kafka
        conf = {
            'bootstrap.servers': 'kafka:9092'
        }

        admin = AdminClient(conf)

        # Создание нового топика для альбомов
        new_topic = NewTopic(
            topic='media_albums',
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
        TOPIC = 'media_albums'

        # Получаем данные из XCom
        print("🔄 Получаем данные альбомов из XCom...")
        
        # Получаем пользователей из XCom
        users = context['task_instance'].xcom_pull(key='users', task_ids='generate_data_group.gen_users')
        print(f"✅ Получено {len(users)} пользователей из XCom")
        
        # Получаем фото, видео и альбомы из XCom
        photos = context['task_instance'].xcom_pull(key='photos', task_ids='generate_data_group.gen_photos')
        videos = context['task_instance'].xcom_pull(key='videos', task_ids='generate_data_group.gen_videos')
        data = context['task_instance'].xcom_pull(key='albums', task_ids='generate_data_group.gen_albums')

        if not data:
            print("❌ Не удалось сгенерировать данные для альбомов")
            return []
        
        print(f"✅ Сгенерировано {len(data)} альбомов для отправки")
        
        # Отправляем данные в Kafka
        for item in data:
            try:
                # Убеждаемся, что данные сериализуются в JSON
                json_data = json.dumps(item, ensure_ascii=False, default=str)
                producer.produce(TOPIC, json_data.encode('utf-8'))
            except Exception as e:
                print(f"Ошибка при отправке альбома {item.get('id', 'unknown')}: {e}")
                continue
                
        producer.flush()
        print(f"✅ Отправлено {len(data)} альбомов в топик {TOPIC}")
        return data
        
    except Exception as e:
        print(f"❌ Критическая ошибка в my_producer_albums: {e}")
        logging.error(f"Ошибка в my_producer_albums: {e}")
        raise
    finally:
        try:
            producer.close()
        except:
            pass
