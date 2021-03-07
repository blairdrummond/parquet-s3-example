#!/bin/python3

import pandas
import pyarrow
from pyarrow import parquet as pq
import s3fs

HOST = 'http://localhost:9000'
SECURE = HOST.startswith('https')
ACCESS_KEY = 'minioadmin' # use getpass or os.getenv
SECRET_KEY = 'minioadmin' # use getpass or os.getenv
BUCKET = 'cars'
PREFIX = 'raw'
assert not PREFIX.endswith("/")

# Configure s3fs for minio
fs = s3fs.S3FileSystem(
    anon=False,
    # key=s3fs_config.aws_access_key_id,
    # secret=s3fs_config.aws_secret_access_key,
    use_ssl=SECURE,
    client_kwargs={
        "region_name": "us-east-1",
        "endpoint_url": HOST,
        "aws_access_key_id": ACCESS_KEY,
        "aws_secret_access_key": SECRET_KEY,
        "verify": SECURE,
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
