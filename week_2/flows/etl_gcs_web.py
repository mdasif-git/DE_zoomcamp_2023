from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import random
import os
#PREFECT_WORK_DIR = "~/DE_zoomcamp_2023/week_2/prefect_flows"

@task(retries=3)
def fetch(url:str) -> pd.DataFrame:
    """
    Read data from web to Pandas Dataframe
    """
    # if random.randint(0,1) > 0:
    #     raise Exception

    df = pd.read_csv(url)
    return df

@task()
def write_local(df:pd.DataFrame,color:str,dataset_file:str) -> Path:
    """Write dataframe locally as parquet"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    print(os.getcwd())
    df.to_parquet(path,compression='gzip')
    return path

@task()
def upload_to_gcs(path:Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_bucket_block = GcsBucket.load("gcs-bucket-connector")
    gcs_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )

@task()
def clean(df:pd.DataFrame,dataset_file:str) -> pd.DataFrame:
    """Fix D-Type warnings"""
    #df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    #df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df
    #path = write_local(df,color,dataset_file)  

@flow()
def etl_web_to_gcs() -> None:
    """
    The main ETL function
    """
    color = 'green'
    year = 2020

    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df,dataset_file)
    file_path = write_local(df_clean,color,dataset_file)
    upload_to_gcs(file_path)

if __name__ == '__main__':
    etl_web_to_gcs()    
