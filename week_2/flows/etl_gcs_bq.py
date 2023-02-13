from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task()
def extract_from_gcs(color: str,file_name: str) -> Path:
    """Download trip data from gcs bucket"""
    gcs_path = f"data/{color}/{file_name}"
    gcs_block = GcsBucket.load("gcs-bucket-connector")
    gcs_block.get_directory(from_path=gcs_path,local_path=f'../data/')
    return Path(f'../data/{gcs_path}')

@task(retries=3)
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    print(f"pre: missing passenger counts: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0,inplace=True)
    print(f"post: missing passenger counts: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_to_bq(df: pd.DataFrame) -> None:
    """Writes DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("prefect-credential")
    df.to_gbq(
        destination_table="de-zoomcamp-2023-375903.dtc_bq.rides",
        project_id="de-zoomcamp-2023-375903",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=100000,
        if_exists='append'
    )


@flow()
def gcs_to_bq():
    color = 'yellow'
    year = 2021
    month = 1
    file_name = f"{color}_tripdata_{year}-{month:02}.parquet"
    path = extract_from_gcs(color,file_name)
    df = transform(path)
    write_to_bq(df)


if __name__ == '__main__':
    gcs_to_bq()     