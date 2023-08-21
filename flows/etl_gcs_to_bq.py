from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pyarrow as pa
from pyarrow.parquet import ParquetFile
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp import GcpCredentials


@task(retries=2)
def extract_from_gcp(color:str, year:int, month:int) -> Path:
    """Extract data from gcp to local"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcp")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task(retries=2)
def transform_data(path:Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    print(f"Pre: Misssing passenger count: {df['passenger_count'].isna().sum() }")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"Post: Misssing passenger count: {df['passenger_count'].isna().sum() }")
    return df


@task(retries=2)
def write_gcp(df:pd.DataFrame) -> None:
    """Write to Gcp"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table="taxi_data.ny_taxi",
        project_id="data-engineering-z",
        chunksize=200000,
        if_exists="append",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
    )


@flow()
def gcs_to_bq(color, year, month):
    path = extract_from_gcp(color, year, month) 
    df = transform_data(path)
    write_gcp(df)
    


if __name__=="__main__":
    color ="yellow"
    year=2021
    month =1
    gcs_to_bq(color, year, month)