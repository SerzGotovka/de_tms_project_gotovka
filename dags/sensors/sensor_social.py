from airflow.sensors.filesystem import FileSensor # type: ignore
import logging
import os
import glob


logger = logging.getLogger()
logger.setLevel('INFO')

DATA_DIR = '/opt/airflow/dags/save_data/social/'
os.makedirs(DATA_DIR, exist_ok=True)

class FileSensorSocial(FileSensor):
    def poke(self, context):
        files = glob.glob(self.filepath)
        if files:
            # Берем самый новый файл
            latest_file = max(files, key=os.path.getctime)
            context['ti'].xcom_push(key='file_path', value=latest_file)
            logger.info(f"Найден файл социальных данных: {latest_file}")
            return True
        return False
