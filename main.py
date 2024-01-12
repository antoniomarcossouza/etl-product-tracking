"""Script de extração dos dados"""

import os
import glob

import pandas as pd

# from joblib import Parallel, delayed


def process_file(csv_file: str):
    """Lê o arquivo CSV, processa as informações e deleta o arquivo"""

    df = pd.read_csv(csv_file)

    print(csv_file)
    print(df)

    # os.remove(csv_file)


if __name__ == "__main__":
    files = glob.glob(os.path.join("./data", "*.csv"))

    # Processa os arquivos de 5 em 5
    for i in range(0, len(files), 5):
        file_list = files[i : i + 5]

        # Parallel(n_jobs=-1)(
        #     delayed(process_file)(arquivo) for arquivo in file_list
        # )

        for file in file_list:
            process_file(file)
