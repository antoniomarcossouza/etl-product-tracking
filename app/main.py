"""Script de extração dos dados"""

import os
import glob

import pandas as pd
import psycopg2

from database import build_query


def process_file(csv_file: str):
    """Lê o arquivo CSV, processa as informações e deleta o arquivo"""

    df = pd.read_csv(csv_file)
    # _ = csv_file.split("/")[-1][:-4]

    connection = psycopg2.connect(
        user="postgres",
        password="root",
        host="localhost",
        port="5432",
        database="product_tracking",
    )

    cursor = connection.cursor()

    for _, row in df.iterrows():
        query = build_query(row)
        print(query)
        # cursor.execute(query)
        break


if __name__ == "__main__":
    files = glob.glob(os.path.join("./data", "*.csv"))

    for file in files:
        process_file(file)
        break
