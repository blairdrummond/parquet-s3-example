#!/bin/python3

import pandas
import pyarrow
from pyarrow import parquet as pq
import s3fs

HOST = 'http://localhost:9000'
SECURE = HOST.startswith('https')
BUCKET = 'cars'
PREFIX = 'raw'
assert not PREFIX.endswith("/")

# Configure s3fs for minio
fs = s3fs.S3FileSystem(
    anon=False,
    # key=s3fs_config.aws_access_key_id,
    # secret=s3fs_config.aws_secret_access_key,
    use_ssl=False,
    client_kwargs={
        "region_name": "us-east-1",
        "endpoint_url": "http://localhost:9000",
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
        "verify": False,
    }
)

table = pq.ParquetDataset(
    f"s3://{BUCKET}/{PREFIX}",
    filesystem=fs,
).read()

print("Full dataframe")
df = table.to_pandas()
print(df.head())


#############################
###  FIlter by partition  ###
#############################

table = pq.ParquetDataset(
    f"s3://{BUCKET}/{PREFIX}",
    filesystem=fs,
    filters=[("year", "=", "2019")]
).read()

print("Filtered dataframe with only 2019 data")
df = table.to_pandas()
print(df.head())
