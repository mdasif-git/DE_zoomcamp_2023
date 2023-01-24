import pandas as pd 
from sqlalchemy import create_engine
from time import time
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    hostname = params.host
    port = params.port
    database = params.db
    table = params.table
    #url = params.url
    file_name = params.filename

    #download csv file
    #file_name = 'yellow_tripdata_2021-01.csv'
    #os.system(f'wget {url}')
    #os.system(f'gzip -d {file_name}')
    engine = create_engine(f'postgresql://{user}:{password}@{hostname}:{port}/{database}')

    engine.connect()

    df_iter = pd.read_csv(file_name,iterator=True,chunksize=100000)

    while True:
        t_start =  time()
        df = next(df_iter)
        #df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        #df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(con=engine,name=table,if_exists='append')
        t_end = time()
        print('inserted another chunk..time taken = %.3f second'% (t_end - t_start))

if __name__ == '__main__':        
    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')
    #user, password, host, post, db name, table name, csv url
    parser.add_argument('--user',help='user name for postgres')
    parser.add_argument('--password',help='password for postgres')
    parser.add_argument('--host',help='hostname for postgres')
    parser.add_argument('--port',help='port for postgres')
    parser.add_argument('--db',help='database name for postgres')
    parser.add_argument('--table',help='table name to write the results in')
    parser.add_argument('--filename',help='file name for the downloaded file')
    #parser.add_argument('--url',help='url for csv file')

    args = parser.parse_args()

    main(args)