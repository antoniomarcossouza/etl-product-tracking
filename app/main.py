"""Script de extração dos dados"""

import csv
import os
import glob

from database import upsert_operations_and_tracking_events
from delivery import Delivery


def process_file(file_path: str):
    """Lê o arquivo CSV, processa as informações e deleta o arquivo"""

    # _ = file_path.split("/")[-1][:-4]

    with open(file_path, "r", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file, delimiter=",", quotechar='"')

        next(reader)
        for row in reader:
            upsert_operations_and_tracking_events(Delivery(row))


if __name__ == "__main__":
    files = glob.glob(os.path.join("./data", "*.csv"))

    for file in files:
        process_file(file)
        break
