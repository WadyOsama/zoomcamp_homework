#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    
    os.system(f'wget {url} -O {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)

    while True:
        start_time = time()
        try:
            df = next(df_iter)
            df.to_sql(name = table_name, con=engine, if_exists='append')
            end_time = time()
            print("Another chunck inserted...\ntook %.3f seconds\n" % (end_time-start_time))
        except StopIteration:
            print("All chunks have been processed and inserted.")
            break

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV files to Postgres')

    parser.add_argument('user', help='username for postgres')
    parser.add_argument('password', help='password for postgres')
    parser.add_argument('host', help='hostname for postgres')
    parser.add_argument('port', help='port for postgres')
    parser.add_argument('db', help='database name for postgres')
    parser.add_argument('table_name', help='table name to store on it')
    parser.add_argument('url', help='url for csv file')

    args = parser.parse_args()

    main(args)