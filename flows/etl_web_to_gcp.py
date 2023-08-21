#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pyarrow as pa
from pyarrow.parquet import ParquetFile
from prefect.tasks import task_input_hash
from datetime import timedelta



@task(retries=3, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web to pandas df"""
    df =  pd.read_parquet(dataset_url, engine='pyarrow') # this gonna consume too much resources alternative below

    # pf=ParquetFile(dataset_url)
    # first_200k_rows = next(pf.iter_batches(batch_size = 200000)) 
    # df = pa.Table.from_batches([first_200k_rows]).to_pandas() 
    return df

@task(retries=3, log_prints=True) 
def clean(df=pd.DataFrame) -> pd.DataFrame: 
    """Fix datatype issues"""
    print(f"Before conversion: {df.dtypes}")
    
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"] )
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"] )
    
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task(retries=3, log_prints=True)
def write_local(df:pd.DataFrame, color:str, dataset_file:str) -> Path:
    """Write Dataframe as parquet file to local"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(retries=3, log_prints=True)
def write_gcp(path: Path) -> None :
    gcp_block = GcsBucket.load("zoom-gcp")
    print(path)
    gcp_block.upload_from_path(path, path)
    return


@flow()
def etl_web_to_gcs() -> None:
    '''Main ETL function'''
    # ex="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
    color ="yellow"
    year = 2021
    month=1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"
    # print(dataset_url)
    df = fetch(dataset_url)
    clean_df = clean(df)
    path = write_local(clean_df, color, dataset_file)
    write_gcp(path)


if __name__ == '__main__':
    etl_web_to_gcs()

print("ss")