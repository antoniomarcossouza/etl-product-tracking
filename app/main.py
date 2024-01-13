"""Script de extração dos dados"""

import ast
import os
import glob
from datetime import datetime

import pandas as pd


def build_sql_insert(table_name: str, columns: list, values: list):
    """Constrói o comando SQL de inserção"""

    columns = ", ".join(columns)
    values = ", ".join(
        [
            f"'{v}'" if isinstance(v, str) else "NULL" if v is None else str(v)
            for v in values
        ]
    )

    return f"INSERT INTO {table_name} ({columns}) VALUES ({values});"


def build_query(row: pd.Series):
    """Constrói a query SQL"""

    op_id = row["oid__id"]
    created_at = datetime.fromtimestamp(row["createdAt"]).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    updated_at = datetime.fromtimestamp(row["updatedAt"]).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    last_sync = datetime.fromtimestamp(row["lastSyncTracker"]).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )

    print(
        build_sql_insert(
            table_name="operations",
            columns=["op_id", "created_at", "updated_at", "last_sync"],
            values=[op_id, created_at, updated_at, last_sync],
        )
    )

    for item in ast.literal_eval(row["array_trackingEvents"]):
        created_at = datetime.fromtimestamp(
            (item["createdAt"]["$date"] / 1000)
        ).strftime("%Y-%m-%d %H:%M:%S.%f")
        tracking_code = item["trackingCode"]
        status = item["status"]
        description = item["description"]
        tracker_type = item["trackerType"]
        origin = item["from"]
        destination = item["to"]

        print(
            build_sql_insert(
                table_name="tracking_events",
                columns=[
                    "op_id",
                    "tracking_code",
                    "created_at",
                    "status",
                    "description",
                    "tracker_type",
                    "from",
                    "to",
                ],
                values=[
                    op_id,
                    tracking_code,
                    created_at,
                    status,
                    description,
                    tracker_type,
                    origin,
                    destination,
                ],
            )
        )


def process_file(csv_file: str):
    """Lê o arquivo CSV, processa as informações e deleta o arquivo"""

    df = pd.read_csv(csv_file)
    # _ = csv_file.split("/")[-1][:-4]

    for _, row in df.iterrows():
        build_query(row)
        break


if __name__ == "__main__":
    files = glob.glob(os.path.join("./data", "*.csv"))

    for file in files:
        process_file(file)
        break
