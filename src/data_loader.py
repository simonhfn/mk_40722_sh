from typing import Union

import boto3
import pandas as pd

S3CLIENT = boto3.client('s3')
BUCKET = "work-sample-mk"

def list_file_from_bucket(bucket):
    for key in S3CLIENT.list_objects(Bucket=bucket)['Contents']:
        yield key['Key']

def get_file(bucket, key) -> pd.DataFrame:
    print(f"getting from s3, file from bucket: {bucket}, and key: {key}")
    obj = S3CLIENT.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj['Body'])


def download_file(bucket, key, output_file) -> None:
    S3CLIENT.download_file(bucket, key, output_file)


def create_file_name(year: Union[str, int], month: Union[str, int]) -> str:
    return f"{year}/{str(month).zfill(2)}/events.csv"


def get_local_file() -> pd.DataFrame:
    try:
        return pd.read_parquet("data/events.parquet")
    except:
        raise FileNotFoundError("pls download file first using download_file method in data_loader.py")


if __name__ == "__main__":
    for file in list_file_from_bucket(BUCKET):
        print(file)