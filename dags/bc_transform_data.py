# 2_transform_data
import pandas as pd
import os
import json



# Task 2a)

def transform_data_into_csv(n_files=2, filename='data.csv',
                            raw_dir="/app/raw_files",
                            clean_dir="/app/clean_data"):


    # Use paths passed as arguments - defaults are container paths
    files = sorted(os.listdir(raw_dir), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:

        full_path = os.path.join(raw_dir, f)

        # skip directories
        if not os.path.isfile(full_path):
            continue

        with open(full_path, 'r') as file:
            data_temp = json.load(file)

        # skip empty files
        if not data_temp:
            continue

        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],      # Due Frech Dashboard localhost:8050!
                    'date': f.split('.')[0]
                }
            )

    df = pd.DataFrame(dfs)
    print('\n', df.head(10))

    os.makedirs(clean_dir, exist_ok=True)
    df.to_csv(os.path.join(clean_dir, filename), index=False)



if __name__ == "__main__":
    transform_data_into_csv(
        n_files=None,
        filename='data_test.csv',
        raw_dir='./raw_files',
        clean_dir='./clean_data'        # name on the filesystem
    )