"""Script de extração dos dados"""

import ast
import json
import os
import glob

import pandas as pd


def process_file(csv_file: str):
    """Lê o arquivo CSV, processa as informações e deleta o arquivo"""

    df = pd.read_csv(csv_file)

    print(csv_file.split("/")[-1][:-4])

    for _, value in df["array_trackingEvents"].items():
        tracking_events = ast.literal_eval(value)
        break

if __name__ == "__main__":
    files = glob.glob(os.path.join("./data", "*.csv"))

    for i in range(0, len(files), 5):
        file_list = files[i : i + 5]

        for file in file_list:
            process_file(file)
            break
        break
