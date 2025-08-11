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


# Путь для временного сохранения csv файлов user (без временной метки)
temp_file_path_users = '/opt/airflow/dags/save_data/users/data_users.csv'

# Имя файла user для сохранения в MINIO (без временной метки)
filename_users = 'data_users.csv'
