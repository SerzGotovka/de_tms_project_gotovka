from airflow.decorators import dag, task
from datetime import datetime
import logging
from generate_data.generate_users import gen_user, generate_all

logger = logging.getLogger(__name__)


@dag(
    dag_id='generate_data',
    start_date=datetime(2025, 8, 4),
    schedule_interval=None,
    tags=['generate'],
    description='Даг для генерации данных пользователей',
    catchup=False
)
def generate_data_dag():
    
    @task
    def generate_users():
        """Генерирует данные пользователей используя gen_user"""
        logger.info("Начинаю генерацию данных пользователей")
        
        try:
            # Используем функцию generate_all, которая внутри использует gen_user
            data = generate_all()
            
            logger.info("Данные успешно сгенерированы:")
            for table, rows in data.items():
                logger.info(f"  {table}: {len(rows)} строк")
            
            return data
            
        except Exception as e:
            logger.error(f"Ошибка при генерации данных: {e}")
            raise
    
    @task
    def process_generated_data(data):
        """Обрабатывает сгенерированные данные"""
        logger.info("Обрабатываю сгенерированные данные")
        
        if data:
            total_users = len(data.get('users', []))
            logger.info(f"Всего сгенерировано пользователей: {total_users}")
            
            # Здесь можно добавить дополнительную обработку данных
            # Например, сохранение в базу данных, отправка в другой сервис и т.д.
            
            return {
                'status': 'success',
                'total_users': total_users,
                'tables': list(data.keys())
            }
        else:
            logger.warning("Нет данных для обработки")
            return {'status': 'no_data'}
    
    # Создаем цепочку задач
    users_data = generate_users()
    result = process_generated_data(users_data)
    
    return result


# Создаем экземпляр DAG
dag = generate_data_dag()
