from datetime import datetime
# Типы реакций
REACTION_TYPES = ('like', 'love', 'haha', 'wow', 'sad', 'angry')

NUM_USERS = 100  # Количество пользователей

BADGE_NAMES = (
    "verified",
    "top-contributor",
    "beta-tester",
    "early-adopter",
    "vip",
    "moderator",
    "donator",
)
PRIVACY_LEVELS = ("public", "friends", "private")
STATUS_OPTIONS = ("online", "offline")


NUM_COMMUNITIES = 5
NUM_GROUPS = 8
MAX_MEMBERS_PER_GROUP = 20
REACTION_TYPES = ("like", "love", "haha", "wow", "sad", "angry")


# Путь для временного сохранения csv файлов user (с временной меткой)
temp_file_path_users = f'/opt/airflow/dags/save_data/users/data_users_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

# Имя файла user для сохранения в MINIO (с временной меткой)
filename_users = f'data_users_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

temp_file_path_profiles = f'/opt/airflow/dags/save_data/users/data_users_profiles_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
filename_users_profiles = f'data_users_profiles_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

# Имя файла groups для сохранения в MINIO (с временной меткой)
filename_groups = f'data_groups_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

# Имя файла social для сохранения в MINIO (с временной меткой)
filename_social = f'data_social_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

# Путь для временного сохранения csv файлов groups (с временной меткой)
temp_file_path_groups = f'/opt/airflow/dags/save_data/groups/data_groups_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

# Путь для временного сохранения csv файлов social (с временной меткой)
temp_file_path_social = f'/opt/airflow/dags/save_data/social/data_social_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

# Пути для временного сохранения csv файлов настроек пользователей
temp_file_path_user_settings = f'/opt/airflow/dags/save_data/users/data_user_settings_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
temp_file_path_user_privacy = f'/opt/airflow/dags/save_data/users/data_user_privacy_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
temp_file_path_user_status = f'/opt/airflow/dags/save_data/users/data_user_status_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

# Пути для временного сохранения csv файлов социальных связей
temp_file_path_friends = f'/opt/airflow/dags/save_data/social/data_friends_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
temp_file_path_followers = f'/opt/airflow/dags/save_data/social/data_followers_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
temp_file_path_subscriptions = f'/opt/airflow/dags/save_data/social/data_subscriptions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
temp_file_path_groups = f'/opt/airflow/dags/save_data/social/data_groups_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
temp_file_path_blocks = f'/opt/airflow/dags/save_data/social/data_blocks_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
temp_file_path_mutes = f'/opt/airflow/dags/save_data/social/data_mutes_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
temp_file_path_close_friends = f'/opt/airflow/dags/save_data/social/data_close_friends_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

# Пути для временного сохранения csv файлов групп
temp_file_path_communities = f'/opt/airflow/dags/save_data/groups/data_communities_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
temp_file_path_community_topics = f'/opt/airflow/dags/save_data/groups/data_community_topics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
temp_file_path_group_members = f'/opt/airflow/dags/save_data/groups/data_group_members_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
temp_file_path_pinned_posts = f'/opt/airflow/dags/save_data/groups/data_pinned_posts_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'