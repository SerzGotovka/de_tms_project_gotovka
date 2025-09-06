from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from utils.function_minio import load_file_to_minio
from sensors.sensor_users import DATA_DIR, FileSensorUser
from sensors.sensor_groups import FileSensorGroups
from sensors.sensor_social import FileSensorSocial
from utils.tg_bot import send_telegram_message
from datetime import datetime, timedelta
import os


def load_users_from_xcom(**context):
    """Функция для загрузки файла users в MINIO, получая путь из XCom"""
    file_path = context['ti'].xcom_pull(task_ids='wait_for_users', key='file_path')
    if not file_path:
        raise ValueError("Путь к файлу не найден в XCom")
    
    # Получаем имя файла из полного пути
    filename = os.path.basename(file_path)
    
    # Загружаем файл в MINIO
    record_count = load_file_to_minio(file_path, filename, folder="/users/")
    
    # Отправляем уведомление в Telegram
    try:
        message = f"""
📁 ФАЙЛ ЗАГРУЖЕН В MINIO

📂 Тип данных: Пользователи
📄 Файл: {filename}
📁 Папка: /users/
📊 Записей: {record_count}
✅ Статус: Успешно загружен
"""
        send_telegram_message(message)
    except Exception as e:
        print(f"Не удалось отправить уведомление в Telegram: {e}")
    
    return f"Файл {filename} успешно загружен в MINIO"


def load_groups_from_xcom(**context):
    """Функция для загрузки файла groups в MINIO, получая путь из XCom"""
    file_path = context['ti'].xcom_pull(task_ids='wait_for_groups', key='file_path')
    if not file_path:
        raise ValueError("Путь к файлу не найден в XCom")
    
    # Получаем имя файла из полного пути
    filename = os.path.basename(file_path)
    
    # Загружаем файл в MINIO
    record_count = load_file_to_minio(file_path, filename, folder="/groups/")
    
    # Отправляем уведомление в Telegram
    try:
        message = f"""
📁 ФАЙЛ ЗАГРУЖЕН В MINIO

📂 Тип данных: Группы
📄 Файл: {filename}
📁 Папка: /groups/
📊 Записей: {record_count}
✅ Статус: Успешно загружен
"""
        send_telegram_message(message)
    except Exception as e:
        print(f"Не удалось отправить уведомление в Telegram: {e}")
    
    return f"Файл {filename} успешно загружен в MINIO"


def load_social_from_xcom(**context):
    """Функция для загрузки файла social в MINIO, получая путь из XCom"""
    file_path = context['ti'].xcom_pull(task_ids='wait_for_social', key='file_path')
    if not file_path:
        raise ValueError("Путь к файлу не найден в XCom")
    
    # Получаем имя файла из полного пути
    filename = os.path.basename(file_path)
    
    # Загружаем файл в MINIO
    record_count = load_file_to_minio(file_path, filename, folder="/social/")
    
    # Отправляем уведомление в Telegram
    try:
        message = f"""
📁 ФАЙЛ ЗАГРУЖЕН В MINIO

📂 Тип данных: Социальные связи
📄 Файл: {filename}
📁 Папка: /social/
📊 Записей: {record_count}
✅ Статус: Успешно загружен
"""
        send_telegram_message(message)
    except Exception as e:
        print(f"Не удалось отправить уведомление в Telegram: {e}")
    
    return f"Файл {filename} успешно загружен в MINIO"


default_args = {
    'retries': 3,  # попробовать 3 раза при неудаче
    'retry_delay': timedelta(minutes=90),
    'schedule_interval': '@daily',
    'start_date': datetime(2025, 6, 26),
    'catchup': False,
    'max_active_runs': 1
}

with DAG(
    'load_data_to_minio',
    description = 'Загрузка файлов users, groups и social в MINIO через сенсоры',
    tags=["load_to_minio","sensors"],
    default_args=default_args
) as dag:
    
    # Сенсор для пользователей
    wait_for_users = FileSensorUser(
        task_id = 'wait_for_users',
        fs_conn_id = 'fs_user',
        filepath = f'{DATA_DIR}data_users_*.csv',
        poke_interval = 30,
        timeout = 30 * 5
    )

    # Сенсор для групп
    wait_for_groups = FileSensorGroups(
        task_id = 'wait_for_groups',
        fs_conn_id = 'fs_user',
        filepath = '/opt/airflow/dags/save_data/groups/data_*.csv',
        poke_interval = 30,
        timeout = 30 * 5
    )

    # Сенсор для социальных данных
    wait_for_social = FileSensorSocial(
        task_id = 'wait_for_social',
        fs_conn_id = 'fs_user',
        filepath = '/opt/airflow/dags/save_data/social/data_*.csv',
        poke_interval = 30,
        timeout = 30 * 5
    )

    # Задача загрузки пользователей
    load_users_task = PythonOperator(
        task_id='load_users',
        python_callable=load_users_from_xcom,
        provide_context=True
    )
    
    # Задача загрузки групп
    load_groups_task = PythonOperator(
        task_id='load_groups',
        python_callable=load_groups_from_xcom,
        provide_context=True
    )
    
    # Задача загрузки социальных данных
    load_social_task = PythonOperator(
        task_id='load_social',
        python_callable=load_social_from_xcom,
        provide_context=True
    )
    
    # Определяем зависимости - все сенсоры работают параллельно
    wait_for_users >> load_users_task
    wait_for_groups >> load_groups_task
    wait_for_social >> load_social_task