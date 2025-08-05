from airflow.decorators import dag
from datetime import datetime
import logging
import sys
import os
from generate_data.generate_users import gen_user

logger = logging.getLogger(__name__)


@dag(
    dag_id='generate_data',
    start_date=datetime(2025, 8, 4),
    schedule_interval=None,
    tags=['generate'],
    description='Даг для генерации всех данных с использованием TensorflowAPI',
    catchup=False
)
def generate_data_dag():
    users = gen_user()
    # user_profiles = gen_user_profile(users)
    # user_settings = gen_user_settings(users)
    # user_privacy = gen_user_privacy(users)
    # user_status = gen_user_status(users)
    # user_badges = gen_user_badges(users)
    # user_notifications = gen_user_notifications(users)

generate_data_dag()

