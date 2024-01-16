"""Módulo para construir a query de inserção no banco de dados"""

import ast
from datetime import datetime

import pandas as pd


def format_date(date: str):
    """Formata a data para o padrão do banco de dados"""

    return datetime.fromtimestamp(date).strftime("%Y-%m-%d %H:%M:%S.%f")


def create_values_string(values: list):
    """Cria uma string com os valores para a query"""

    return ", ".join(
        [
            f"'{v}'" if isinstance(v, str) else "NULL" if v is None else str(v)
            for v in values
        ]
    )


def build_query(row: pd.Series):
    """Constrói a query para inserir a linha do DataFrame"""

    operation_values = [
        row["oid__id"],
        format_date(row["createdAt"]),
        format_date(row["updatedAt"]),
        format_date(row["lastSyncTracker"]),
    ]

    query = "BEGIN;\n"
    query += "INSERT INTO st_operations (id, created_at, updated_at, last_sync_tracker)\n"
    query += f"VALUES ({create_values_string(operation_values)});\n\n"

    query += "UPDATE t_operations\n"
    query += "SET updated_at = st_operations.updated_at, last_sync_tracker = st_operations.last_sync_tracker\n"
    query += "FROM st_operations\n"
    query += "WHERE t_operations.id = st_operations.id;\n\n"

    query += "INSERT INTO t_operations\n"
    query += "SELECT st_operations.*\n"
    query += "FROM st_operations\n"
    query += "LEFT JOIN t_operations ON t_operations.id = st_operations.id\n"
    query += "WHERE t_operations.id IS NULL;\n\n"

    query += "DELETE FROM st_operations;\n"




    # REFATORAR EVENTOS

    # for item in ast.literal_eval(row["array_trackingEvents"]):
    #     created_at = format_date(item["createdAt"]["$date"] / 1000)
    #     tracking_code = item["trackingCode"]
    #     status = item["status"]
    #     description = item["description"]
    #     tracker_type = item["trackerType"]
    #     origin = item["from"]
    #     destination = item["to"]

    #     tracking_event = create_upsert(
    #         table_name="tracking_events",
    #         columns=[
    #             "operation_id",
    #             "tracking_code",
    #             "created_at",
    #             "status",
    #             "description",
    #             "tracker_type",
    #             "origin",
    #             "destination",
    #         ],
    #         values=[
    #             op_id,
    #             tracking_code,
    #             created_at,
    #             status,
    #             description,
    #             tracker_type,
    #             origin,
    #             destination,
    #         ],
    #     )

    #     query += f"{chr(9)}{tracking_event}{chr(10)}"

    query += "COMMIT;"
    return query
