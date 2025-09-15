from airflow.models import Variable # type: ignore
from neo4j import GraphDatabase
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
import logging
from typing import List, Dict, Any
import pandas as pd


def get_neo4j_connection():
    """Получение подключения к Neo4j"""
    uri = Variable.get("neo4j_uri")
    username = Variable.get("neo4j_username")
    password = Variable.get("neo4j_password")
    return GraphDatabase.driver(uri,auth=(username, password))


def insert_users_to_neo4j(**context) -> int:
    """
    Загрузка данных пользователей в Neo4j из PostgreSQL
    Args:
        **context: Контекст Airflow
    Returns:
        int: Количество загруженных записей
    """
    
    
    # Получаем данные пользователей из PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    
    try:
        # Запрашиваем данные пользователей из DDS слоя
        query = """
        SELECT id, username, email, created_at, last_login, is_active, is_verified
        FROM dds.users
        ORDER BY created_at DESC
        LIMIT 1000
        """
        
        users_data = postgres_hook.get_records(query)
        
        if not users_data:
            logging.warning("Нет данных пользователей для загрузки в Neo4j")
            return 0
            
        # Преобразуем в список словарей
        users_list = []
        for row in users_data:
            users_list.append({
                'id': row[0],
                'username': row[1],
                'email': row[2],
                'created_at': row[3],
                'last_login': row[4],
                'is_active': row[5],
                'is_verified': row[6]
            })
        
        logging.info(f"Получено {len(users_list)} пользователей из PostgreSQL для загрузки в Neo4j")
        
    except Exception as e:
        logging.error(f"Ошибка при получении данных пользователей из PostgreSQL: {str(e)}")
        return 0
    
    driver = get_neo4j_connection()
    inserted_count = 0
    
    try:
        with driver.session() as session:
            for user in users_list:
                session.run(
                    """
                    MERGE (u:User {id: $user_id})
                    SET u.username = $username,
                        u.email = $email,
                        u.created_at = $created_at,
                        u.last_login = $last_login,
                        u.is_active = $is_active,
                        u.is_verified = $is_verified
                    """,
                    user_id=user['id'],
                    username=user['username'],
                    email=user['email'],
                    created_at=user['created_at'],
                    last_login=user['last_login'],
                    is_active=user['is_active'],
                    is_verified=user['is_verified']
                )
                inserted_count += 1
            
            logging.info(f'Загружено {inserted_count} пользователей в Neo4j')
            
    except Exception as e:
        logging.error(f'Ошибка при загрузке пользователей в Neo4j: {str(e)}')
        raise
    finally:
        driver.close()
    
    return inserted_count



    