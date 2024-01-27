"""Módulo para construir a query de inserção no banco de dados"""
import os
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def create_database_connection() -> psycopg2.extensions.connection:
    """Retorna uma conexão com o banco de dados"""

    return psycopg2.connect(
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
    )


def execute_query(query: str, params: tuple, many: bool = False) -> None:
    """Função que executa uma query no banco de dados"""
    connection = None

    try:
        connection = create_database_connection()
        cursor = connection.cursor()

        if many:
            cursor.executemany(query, params)
        else:
            cursor.execute(query, params)

        connection.commit()
        cursor.close()
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def check_already_processed(file_name: str) -> bool:
    """Função que verifica se o arquivo já foi processado"""

    connection = None
    try:
        connection = create_database_connection()
        cur = connection.cursor()

        cur.execute(
            """
            SELECT EXISTS(
                SELECT 1 FROM t_processed_files WHERE created_at = %s
            )
            """,
            (datetime.strptime(file_name, "%Y%m%d-%H%M%S%f"),),
        )

        return cur.fetchone()[0]

    except psycopg2.Error as e:
        print("Unable to connect to the database")
        print(e)
        return False

    finally:
        if cur:
            cur.close()
        if connection:
            connection.close()


def log_processed_file(
    file_name: str, started_at: datetime, concluded_at: datetime
) -> None:
    """Função que insere o nome do arquivo processado no banco de dados"""

    execute_query(
        """
        INSERT INTO t_processed_files (created_at, started_at,concluded_at)
        VALUES (%s,%s,%s)
        """,
        (
            datetime.strptime(file_name, "%Y%m%d-%H%M%S%f"),
            started_at,
            concluded_at,
        ),
    )


def upsert_operations_and_tracking_events(data: list) -> None:
    """Função que executa a procedure no banco de dados"""

    execute_query(
        """
        CALL sp_upsert_deliveries_and_tracking_events(
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        data,
        many=True,
    )
