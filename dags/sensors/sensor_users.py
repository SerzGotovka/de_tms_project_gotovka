from airflow.sensors.filesystem import FileSensor # type: ignore
import logging
import os
import glob


logger = logging.getLogger()
logger.setLevel('INFO')

DATA_DIR = '/opt/airflow/dags/save_data/users/'
os.makedirs(DATA_DIR, exist_ok=True)

class FileSensorWithXCom(FileSensor):
    def poke(self, context):
        files = glob.glob(self.filepath)
        if files:
            context['ti'].xcom_push(key='file_path', value=files[0])
            return True
        return False
    
wait_for_users = FileSensorWithXCom(
        task_id = 'wait_for_users',
        fs_conn_id = 'fs_user',
        filepath = f'{DATA_DIR}data_users_*.csv',
        poke_interval = 30,
        timeout = 30 * 5
    )


