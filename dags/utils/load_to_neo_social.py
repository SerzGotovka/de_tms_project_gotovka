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
    return GraphDatabase.driver(uri, auth=(username, password))


def load_friends_to_neo4j(**context) -> int:
    """
    Загрузка дружеских связей в Neo4j из PostgreSQL
    Args:
        **context: Контекст Airflow
    Returns:
        int: Количество загруженных записей
    """
    postgres_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    
    try:
        # Запрашиваем данные дружеских связей из DDS слоя
        query = """
        SELECT id, user_id, friend_id, created_at, status, is_best_friend
        FROM dds.friends
        ORDER BY created_at DESC
        LIMIT 1000
        """
        
        friends_data = postgres_hook.get_records(query)
        
        if not friends_data:
            logging.warning("Нет данных дружеских связей для загрузки в Neo4j")
            return 0
            
        # Преобразуем в список словарей
        friends_list = []
        for row in friends_data:
            friends_list.append({
                'id': row[0],
                'user_id': row[1],
                'friend_id': row[2],
                'created_at': row[3],
                'status': row[4],
                'is_best_friend': row[5]
            })
        
        logging.info(f"Получено {len(friends_list)} дружеских связей из PostgreSQL для загрузки в Neo4j")
        
    except Exception as e:
        logging.error(f"Ошибка при получении данных дружеских связей из PostgreSQL: {str(e)}")
        return 0
    
    driver = get_neo4j_connection()
    inserted_count = 0
    
    try:
        with driver.session() as session:
            for friend in friends_list:
                session.run(
                    """
                    MATCH (u1:User {id: $user_id})
                    MATCH (u2:User {id: $friend_id})
                    MERGE (u1)-[r:FRIENDS {id: $relationship_id}]->(u2)
                    SET r.created_at = $created_at,
                        r.status = $status,
                        r.is_best_friend = $is_best_friend
                    """,
                    user_id=friend['user_id'],
                    friend_id=friend['friend_id'],
                    relationship_id=friend['id'],
                    created_at=friend['created_at'],
                    status=friend['status'],
                    is_best_friend=friend['is_best_friend']
                )
                inserted_count += 1
            
            logging.info(f'Загружено {inserted_count} дружеских связей в Neo4j')
            
    except Exception as e:
        logging.error(f'Ошибка при загрузке дружеских связей в Neo4j: {str(e)}')
        raise
    finally:
        driver.close()
    
    return inserted_count


def load_followers_to_neo4j(**context) -> int:
    """
    Загрузка подписчиков в Neo4j из PostgreSQL
    Args:
        **context: Контекст Airflow
    Returns:
        int: Количество загруженных записей
    """
    postgres_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    
    try:
        # Запрашиваем данные подписчиков из DDS слоя
        query = """
        SELECT id, user_id, follower_id, followed_at, is_active
        FROM dds.followers
        ORDER BY followed_at DESC
        LIMIT 1000
        """
        
        followers_data = postgres_hook.get_records(query)
        
        if not followers_data:
            logging.warning("Нет данных подписчиков для загрузки в Neo4j")
            return 0
            
        # Преобразуем в список словарей
        followers_list = []
        for row in followers_data:
            followers_list.append({
                'id': row[0],
                'user_id': row[1],
                'follower_id': row[2],
                'followed_at': row[3],
                'is_active': row[4]
            })
        
        logging.info(f"Получено {len(followers_list)} подписчиков из PostgreSQL для загрузки в Neo4j")
        
    except Exception as e:
        logging.error(f"Ошибка при получении данных подписчиков из PostgreSQL: {str(e)}")
        return 0
    
    driver = get_neo4j_connection()
    inserted_count = 0
    
    try:
        with driver.session() as session:
            for follower in followers_list:
                session.run(
                    """
                    MATCH (u1:User {id: $user_id})
                    MATCH (u2:User {id: $follower_id})
                    MERGE (u2)-[r:FOLLOWS {id: $relationship_id}]->(u1)
                    SET r.followed_at = $followed_at,
                        r.is_active = $is_active
                    """,
                    user_id=follower['user_id'],
                    follower_id=follower['follower_id'],
                    relationship_id=follower['id'],
                    followed_at=follower['followed_at'],
                    is_active=follower['is_active']
                )
                inserted_count += 1
            
            logging.info(f'Загружено {inserted_count} подписчиков в Neo4j')
            
    except Exception as e:
        logging.error(f'Ошибка при загрузке подписчиков в Neo4j: {str(e)}')
        raise
    finally:
        driver.close()
    
    return inserted_count


def load_subscriptions_to_neo4j(**context) -> int:
    """
    Загрузка подписок в Neo4j из PostgreSQL
    Args:
        **context: Контекст Airflow
    Returns:
        int: Количество загруженных записей
    """
    postgres_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    
    try:
        # Запрашиваем данные подписок из DDS слоя
        query = """
        SELECT id, user_id, subscribed_to, subscribed_at, is_active
        FROM dds.subscriptions
        ORDER BY subscribed_at DESC
        LIMIT 1000
        """
        
        subscriptions_data = postgres_hook.get_records(query)
        
        if not subscriptions_data:
            logging.warning("Нет данных подписок для загрузки в Neo4j")
            return 0
            
        # Преобразуем в список словарей
        subscriptions_list = []
        for row in subscriptions_data:
            subscriptions_list.append({
                'id': row[0],
                'user_id': row[1],
                'subscribed_to': row[2],
                'subscribed_at': row[3],
                'is_active': row[4]
            })
        
        logging.info(f"Получено {len(subscriptions_list)} подписок из PostgreSQL для загрузки в Neo4j")
        
    except Exception as e:
        logging.error(f"Ошибка при получении данных подписок из PostgreSQL: {str(e)}")
        return 0
    
    driver = get_neo4j_connection()
    inserted_count = 0
    
    try:
        with driver.session() as session:
            for subscription in subscriptions_list:
                session.run(
                    """
                    MATCH (u1:User {id: $user_id})
                    MATCH (u2:User {id: $subscribed_to})
                    MERGE (u1)-[r:SUBSCRIBES {id: $relationship_id}]->(u2)
                    SET r.subscribed_at = $subscribed_at,
                        r.is_active = $is_active
                    """,
                    user_id=subscription['user_id'],
                    subscribed_to=subscription['subscribed_to'],
                    relationship_id=subscription['id'],
                    subscribed_at=subscription['subscribed_at'],
                    is_active=subscription['is_active']
                )
                inserted_count += 1
            
            logging.info(f'Загружено {inserted_count} подписок в Neo4j')
            
    except Exception as e:
        logging.error(f'Ошибка при загрузке подписок в Neo4j: {str(e)}')
        raise
    finally:
        driver.close()
    
    return inserted_count


def load_blocks_to_neo4j(**context) -> int:
    """
    Загрузка блокировок в Neo4j из PostgreSQL
    Args:
        **context: Контекст Airflow
    Returns:
        int: Количество загруженных записей
    """
    postgres_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    
    try:
        # Запрашиваем данные блокировок из DDS слоя
        query = """
        SELECT id, user_id, blocked_id, blocked_at, reason, is_active
        FROM dds.blocks
        ORDER BY blocked_at DESC
        LIMIT 1000
        """
        
        blocks_data = postgres_hook.get_records(query)
        
        if not blocks_data:
            logging.warning("Нет данных блокировок для загрузки в Neo4j")
            return 0
            
        # Преобразуем в список словарей
        blocks_list = []
        for row in blocks_data:
            blocks_list.append({
                'id': row[0],
                'user_id': row[1],
                'blocked_id': row[2],
                'blocked_at': row[3],
                'reason': row[4],
                'is_active': row[5]
            })
        
        logging.info(f"Получено {len(blocks_list)} блокировок из PostgreSQL для загрузки в Neo4j")
        
    except Exception as e:
        logging.error(f"Ошибка при получении данных блокировок из PostgreSQL: {str(e)}")
        return 0
    
    driver = get_neo4j_connection()
    inserted_count = 0
    
    try:
        with driver.session() as session:
            for block in blocks_list:
                session.run(
                    """
                    MATCH (u1:User {id: $user_id})
                    MATCH (u2:User {id: $blocked_id})
                    MERGE (u1)-[r:BLOCKS {id: $relationship_id}]->(u2)
                    SET r.blocked_at = $blocked_at,
                        r.reason = $reason,
                        r.is_active = $is_active
                    """,
                    user_id=block['user_id'],
                    blocked_id=block['blocked_id'],
                    relationship_id=block['id'],
                    blocked_at=block['blocked_at'],
                    reason=block['reason'],
                    is_active=block['is_active']
                )
                inserted_count += 1
            
            logging.info(f'Загружено {inserted_count} блокировок в Neo4j')
            
    except Exception as e:
        logging.error(f'Ошибка при загрузке блокировок в Neo4j: {str(e)}')
        raise
    finally:
        driver.close()
    
    return inserted_count


def load_mutes_to_neo4j(**context) -> int:
    """
    Загрузка отключенных уведомлений (mutes) в Neo4j из PostgreSQL
    Args:
        **context: Контекст Airflow
    Returns:
        int: Количество загруженных записей
    """
    postgres_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    
    try:
        # Запрашиваем данные отключенных уведомлений из DDS слоя
        query = """
        SELECT id, user_id, muted_id, muted_at, duration_days, is_active
        FROM dds.mutes
        ORDER BY muted_at DESC
        LIMIT 1000
        """
        
        mutes_data = postgres_hook.get_records(query)
        
        if not mutes_data:
            logging.warning("Нет данных отключенных уведомлений для загрузки в Neo4j")
            return 0
            
        # Преобразуем в список словарей
        mutes_list = []
        for row in mutes_data:
            mutes_list.append({
                'id': row[0],
                'user_id': row[1],
                'muted_id': row[2],
                'muted_at': row[3],
                'duration_days': row[4],
                'is_active': row[5]
            })
        
        logging.info(f"Получено {len(mutes_list)} отключенных уведомлений из PostgreSQL для загрузки в Neo4j")
        
    except Exception as e:
        logging.error(f"Ошибка при получении данных отключенных уведомлений из PostgreSQL: {str(e)}")
        return 0
    
    driver = get_neo4j_connection()
    inserted_count = 0
    
    try:
        with driver.session() as session:
            for mute in mutes_list:
                session.run(
                    """
                    MATCH (u1:User {id: $user_id})
                    MATCH (u2:User {id: $muted_id})
                    MERGE (u1)-[r:MUTES {id: $relationship_id}]->(u2)
                    SET r.muted_at = $muted_at,
                        r.duration_days = $duration_days,
                        r.is_active = $is_active
                    """,
                    user_id=mute['user_id'],
                    muted_id=mute['muted_id'],
                    relationship_id=mute['id'],
                    muted_at=mute['muted_at'],
                    duration_days=mute['duration_days'],
                    is_active=mute['is_active']
                )
                inserted_count += 1
            
            logging.info(f'Загружено {inserted_count} отключенных уведомлений в Neo4j')
            
    except Exception as e:
        logging.error(f'Ошибка при загрузке отключенных уведомлений в Neo4j: {str(e)}')
        raise
    finally:
        driver.close()
    
    return inserted_count


def load_close_friends_to_neo4j(**context) -> int:
    """
    Загрузка близких друзей (close friends) в Neo4j из PostgreSQL
    Args:
        **context: Контекст Airflow
    Returns:
        int: Количество загруженных записей
    """
    postgres_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    
    try:
        # Запрашиваем данные близких друзей из DDS слоя
        query = """
        SELECT id, user_id, close_friend_id, added_at, is_active
        FROM dds.close_friends
        ORDER BY added_at DESC
        LIMIT 1000
        """
        
        close_friends_data = postgres_hook.get_records(query)
        
        if not close_friends_data:
            logging.warning("Нет данных близких друзей для загрузки в Neo4j")
            return 0
            
        # Преобразуем в список словарей
        close_friends_list = []
        for row in close_friends_data:
            close_friends_list.append({
                'id': row[0],
                'user_id': row[1],
                'close_friend_id': row[2],
                'added_at': row[3],
                'is_active': row[4]
            })
        
        logging.info(f"Получено {len(close_friends_list)} близких друзей из PostgreSQL для загрузки в Neo4j")
        
    except Exception as e:
        logging.error(f"Ошибка при получении данных близких друзей из PostgreSQL: {str(e)}")
        return 0
    
    driver = get_neo4j_connection()
    inserted_count = 0
    
    try:
        with driver.session() as session:
            for close_friend in close_friends_list:
                session.run(
                    """
                    MATCH (u1:User {id: $user_id})
                    MATCH (u2:User {id: $close_friend_id})
                    MERGE (u1)-[r:CLOSE_FRIENDS {id: $relationship_id}]->(u2)
                    SET r.added_at = $added_at,
                        r.is_active = $is_active
                    """,
                    user_id=close_friend['user_id'],
                    close_friend_id=close_friend['close_friend_id'],
                    relationship_id=close_friend['id'],
                    added_at=close_friend['added_at'],
                    is_active=close_friend['is_active']
                )
                inserted_count += 1
            
            logging.info(f'Загружено {inserted_count} близких друзей в Neo4j')
            
    except Exception as e:
        logging.error(f'Ошибка при загрузке близких друзей в Neo4j: {str(e)}')
        raise
    finally:
        driver.close()
    
    return inserted_count


def load_all_social_relationships_to_neo4j(**context) -> Dict[str, int]:
    """
    Загрузка всех социальных связей в Neo4j из PostgreSQL
    Args:
        **context: Контекст Airflow
    Returns:
        Dict[str, int]: Словарь с количеством загруженных записей по типам связей
    """
    results = {}
    
    try:
        # Загружаем все типы социальных связей
        results['friends'] = load_friends_to_neo4j(**context)
        results['followers'] = load_followers_to_neo4j(**context)
        results['subscriptions'] = load_subscriptions_to_neo4j(**context)
        results['blocks'] = load_blocks_to_neo4j(**context)
        results['mutes'] = load_mutes_to_neo4j(**context)
        results['close_friends'] = load_close_friends_to_neo4j(**context)
        
        total_relationships = sum(results.values())
        logging.info(f'Всего загружено {total_relationships} социальных связей в Neo4j')
        logging.info(f'Детализация: {results}')
        
        # Сохраняем результаты в XCom
        context["task_instance"].xcom_push(key="social_relationships_loaded", value=results)
        context["task_instance"].xcom_push(key="total_social_relationships", value=total_relationships)
        
    except Exception as e:
        logging.error(f'Ошибка при загрузке социальных связей в Neo4j: {str(e)}')
        raise
    
    return results

