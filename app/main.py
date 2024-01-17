"""Script de extração dos dados"""

import os
import glob

import pandas as pd

from database import upsert_operations_and_tracking_events


def process_file(csv_file: str):
    """Lê o arquivo CSV, processa as informações e deleta o arquivo"""

    df = pd.read_csv(csv_file)
    # _ = csv_file.split("/")[-1][:-4]

    for _, row in df.iterrows():
        upsert_operations_and_tracking_events(row)


if __name__ == "__main__":
    files = glob.glob(os.path.join("./data", "*.csv"))

    for file in files:
        process_file(file)
