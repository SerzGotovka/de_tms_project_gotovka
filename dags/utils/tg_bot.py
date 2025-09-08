import requests
from airflow.models import Variable # type: ignore
import time


try:
    TELEGRAM_TOKEN = Variable.get("telegram_token")  
    CHAT_ID = Variable.get("chat_id")  
except Exception as e:
    print(f"Ошибка при получении переменных из Airflow: {e}")
    print("Используются значения по умолчанию")
    
    TELEGRAM_TOKEN = None
    CHAT_ID = None

# Функция отправки сообщения в Telegram
def send_telegram_message(message):
    # Проверяем, что токен и chat_id настроены
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print(f"Ошибка: TELEGRAM_TOKEN или CHAT_ID не настроены!")
        print(f"TELEGRAM_TOKEN: {'установлен' if TELEGRAM_TOKEN else 'НЕ УСТАНОВЛЕН'}")
        print(f"CHAT_ID: {'установлен' if CHAT_ID else 'НЕ УСТАНОВЛЕН'}")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            'chat_id': CHAT_ID,
            'text': message,
        }
        response = requests.post(url, json=payload)
        
        # Проверяем статус ответа
        if response.status_code == 200:
            print(f"Сообщение успешно отправлено в Telegram: {message[:50]}...")
            return True
        elif response.status_code == 429:
                # Получаем время ожидания из ответа
                retry_after = int(response.json().get('parameters', {}).get('retry_after', 25))
                print(f"Слишком много запросов. Ожидание {retry_after} секунд перед новой попыткой...")
                time.sleep(retry_after)
        else:
            print(f"Ошибка при отправке сообщения в Telegram. Статус: {response.status_code}")
            print(f"Ответ: {response.text}")
            return False
            
    except Exception as e:
        print(f"Ошибка при отправке сообщения в Telegram: {str(e)}")
        return False
    


# Callback функция для успешного выполнения
def success_callback(context):
    task_instance = context['task_instance']
    task_id = task_instance.task_id

    # Задачи пользователей
    if task_id == 'generate_data_group.gen_users':
        num_users = task_instance.xcom_pull(task_ids='generate_data_group.gen_users', key='num_users')
        message = f"Сгенерировано пользователей: {num_users}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление о генерации пользователей")
    
    elif task_id == 'generate_data_group.gen_profiles':
        num_profiles = task_instance.xcom_pull(task_ids='generate_data_group.gen_profiles', key='num_profiles')
        message = f"Сгенерировано профилей: {num_profiles}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление о генерации профилей")
    
    elif task_id == 'generate_data_group.gen_settings':
        num_settings = task_instance.xcom_pull(task_ids='generate_data_group.gen_settings', key='num_settings')
        message = f"Сгенерировано настроек пользователей: {num_settings}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_privacy':
        num_privacy = task_instance.xcom_pull(task_ids='generate_data_group.gen_privacy', key='num_privacies')
        message = f"Сгенерировано настроек приватности: {num_privacy}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_status':
        num_status = task_instance.xcom_pull(task_ids='generate_data_group.gen_status', key='num_statuses')
        message = f"Сгенерировано статусов пользователей: {num_status}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    # Задачи групп
    elif task_id == 'generate_data_group.gen_communities':
        num_communities = task_instance.xcom_pull(task_ids='generate_data_group.gen_communities', key='num_communities')
        message = f"Сгенерировано сообществ: {num_communities}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_groups':
        num_groups = task_instance.xcom_pull(task_ids='generate_data_group.gen_groups', key='num_groups')
        message = f"Сгенерировано групп: {num_groups}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_group_members':
        num_members = task_instance.xcom_pull(task_ids='generate_data_group.gen_group_members', key='num_group_members')
        message = f"Сгенерировано участников групп: {num_members}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_community_topics':
        num_topics = task_instance.xcom_pull(task_ids='generate_data_group.gen_community_topics', key='num_community_topics')
        message = f"Сгенерировано тем сообществ: {num_topics}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_pinned_posts':
        num_pinned = task_instance.xcom_pull(task_ids='generate_data_group.gen_pinned_posts', key='num_pinned_posts')
        message = f"Сгенерировано закрепленных постов: {num_pinned}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
        
    # Задачи социальных связей
    elif task_id == 'generate_data_group.gen_friends':
        num_friends = task_instance.xcom_pull(task_ids='generate_data_group.gen_friends', key='num_friends')
        message = f"Сгенерировано дружеских связей: {num_friends}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_followers':
        num_followers = task_instance.xcom_pull(task_ids='generate_data_group.gen_followers', key='num_followers')
        message = f"Сгенерировано подписчиков: {num_followers}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_subscriptions':
        num_subscriptions = task_instance.xcom_pull(task_ids='generate_data_group.gen_subscriptions', key='num_subscriptions')
        message = f"Сгенерировано подписок: {num_subscriptions}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_blocks':
        num_blocks = task_instance.xcom_pull(task_ids='generate_data_group.gen_blocks', key='num_blocks')
        message = f"Сгенерировано блокировок: {num_blocks}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_mutes':
        num_mutes = task_instance.xcom_pull(task_ids='generate_data_group.gen_mutes', key='num_mutes')
        message = f"Сгенерировано отключенных уведомлений: {num_mutes}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_close_friends':
        num_close_friends = task_instance.xcom_pull(task_ids='generate_data_group.gen_close_friends', key='num_close_friends')
        message = f"Сгенерировано близких друзей: {num_close_friends}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    # Задачи контента
    elif task_id == 'generate_data_group.gen_posts':
        num_posts = task_instance.xcom_pull(task_ids='generate_data_group.gen_posts', key='num_posts')
        message = f"Сгенерировано постов: {num_posts}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_stories':
        num_stories = task_instance.xcom_pull(task_ids='generate_data_group.gen_stories', key='num_stories')
        message = f"Сгенерировано историй: {num_stories}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_reels':
        num_reels = task_instance.xcom_pull(task_ids='generate_data_group.gen_reels', key='num_reels')
        message = f"Сгенерировано Reels: {num_reels}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_comments':
        num_comments = task_instance.xcom_pull(task_ids='generate_data_group.gen_comments', key='num_comments')
        message = f"Сгенерировано комментариев: {num_comments}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_replies':
        num_replies = task_instance.xcom_pull(task_ids='generate_data_group.gen_replies', key='num_replies')
        message = f"Сгенерировано ответов на комментарии: {num_replies}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_likes':
        num_likes = task_instance.xcom_pull(task_ids='generate_data_group.gen_likes', key='num_likes')
        message = f"Сгенерировано лайков: {num_likes}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_reactions':
        num_reactions = task_instance.xcom_pull(task_ids='generate_data_group.gen_reactions', key='num_reactions')
        message = f"Сгенерировано реакций: {num_reactions}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'generate_data_group.gen_shares':
        num_shares = task_instance.xcom_pull(task_ids='generate_data_group.gen_shares', key='num_shares')
        message = f"Сгенерировано репостов: {num_shares}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    # Задача медиа
    elif task_id == 'generate_data_group.gen_photos':
        num_photos = task_instance.xcom_pull(task_ids='generate_data_group.gen_photos', key='number_photos')
        message = f"Сгенерировано фото: {num_photos}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")

    elif task_id == 'generate_data_group.gen_videos':
        num_videos = task_instance.xcom_pull(task_ids='generate_data_group.gen_videos', key='number_videos')
        message = f"Сгенерировано видео: {num_videos}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")

    elif task_id == 'generate_data_group.gen_albums':
        num_albums = task_instance.xcom_pull(task_ids='generate_data_group.gen_albums', key='number_albums')
        message = f"Сгенерировано альбомов: {num_albums}"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")

    
    # Задачи Kafka
    elif task_id == 'send_media_to_kafka':
        message = "Медиа данные успешно отправлены в Kafka"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    elif task_id == 'send_content_to_kafka':
        message = "Контент данные успешно отправлены в Kafka"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    # Задача создания топиков Kafka
    elif task_id == 'create_kafka_topics':
        message = "Топики Kafka успешно созданы"
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")
    
    else:
        message = f"Задача {task_id} завершена успешно."
        if not send_telegram_message(message):
            print(f"Не удалось отправить уведомление для задачи {task_id}")


# Callback функция для обработки ошибок
def on_failure_callback(context):
    """
    Функция отправляет уведомление в Telegram при возникновении ошибки.
    """
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    
    # Получаем информацию об ошибке
    exception = context.get('exception')
    log_url = context.get('log_url', 'Недоступно')
    
    # Формируем сообщение об ошибке
    error_message = f"""
🚨 **ОШИБКА В ЗАДАЧЕ AIRFLOW** 

**DAG:** {dag_id}
**Задача:** {task_id}
**Дата выполнения:** {execution_date}
**Время ошибки:** {context.get('ts', 'Неизвестно')}

**Описание ошибки:**
{str(exception) if exception else 'Неизвестная ошибка'}

**Логи:** {log_url}

    """
    
    try:
        if send_telegram_message(error_message):
            print(f"Уведомление об ошибке отправлено в Telegram для задачи {task_id}")
        else:
            print(f"Не удалось отправить уведомление об ошибке в Telegram для задачи {task_id}")
    except Exception as e:
        print(f"Ошибка при отправке уведомления в Telegram: {e}")
        print(f"Ошибка в задаче {task_id}: {exception}")



