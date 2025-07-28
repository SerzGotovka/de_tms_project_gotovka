import psycopg2
from clickhouse_driver import Client
from neo4j import GraphDatabase
from confluent_kafka.admin import AdminClient, NewTopic


# Функция для получения версии PostgreSQL
def get_postgresql_version(cursor):
    cursor.execute("SELECT version();")
    return cursor.fetchone()[0]

# PostgreSQL
pg_conn = psycopg2.connect(
    host="localhost",
    port=5440,
    user="airflow",
    password="airflow",
    dbname="airflow"
)
pg_cursor = pg_conn.cursor()
print("PostgreSQL Version:", get_postgresql_version(pg_cursor))
pg_conn.commit()
pg_cursor.close()
pg_conn.close()


# Функция для получения версии ClickHouse
def get_clickhouse_version(client):
    return client.execute("SELECT version()")[0][0]

ch_client = Client(
    host='localhost',
    user='airflow',
    password='airflow',
    port=9000  # TCP порт ClickHouse
)
print("ClickHouse Version:", get_clickhouse_version(ch_client))

   
# Neo4j
def check_neo4j_connection(uri, user, password):
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            version = session.run("CALL dbms.components()").data()
            driver.close()
            return version
    except Exception as e:
        return str(e)

neo4j_uri = "bolt://localhost:7687"  
neo4j_user = "neo4j"
neo4j_password = "password"
neo4j_version = check_neo4j_connection(neo4j_uri, neo4j_user, neo4j_password)
print("Neo4j Version:", neo4j_version)

# Kafka
def check_kafka_connection():
    conf = {
        'bootstrap.servers': 'localhost:9092'
    }
    admin = AdminClient(conf)
    new_topic = NewTopic('test_topic5', num_partitions=1, replication_factor=1)

    fs = admin.create_topics([new_topic])

    for topic, f in fs.items():
        try:
            f.result()
            print(f"[✓] Топик '{topic}' создан")
        except Exception as e:
            print(f"[✗] Ошибка при создании топика '{topic}': {e}")

    topics = admin.list_topics(timeout=5).topics
    print("Существующие топики:", list(topics.keys()))

check_kafka_connection()

