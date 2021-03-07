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

# https://arrow.apache.org/docs/python/parquet.html

df = pandas.read_csv("car.csv")

# Pretend that this is time-series data
df['year']  = 0
df['month'] = 0



###########################################
###   Group the dataset into groups     ###
###   of size $batch_size per month.    ###
###   Keep winding backwards in time    ###
###   until we're through the dataset.  ###
###########################################

# 65000 rows, so batch_size of 1000
# gives 5-6 years worth of data
batch_size = 1000
count = {
    'year' : 2020,
    'month' : 12,
    'batch' : batch_size+1
}
for (i, row) in df.iterrows():
    # Update the counter
    count['batch'] -= 1
    if count['batch'] == 0:
        # reset batch, decrement month
        count['batch'] = batch_size
        count['month'] -= 1
        # NOTE: month ∈ {1,2,...,12}
        if count['month'] == 0:
            # reset month, decrement year
            count['month'] = 12
            count['year'] -= 1
            # No modulo logic on the year.

    # mutate the row
    df.at[i, 'year'] = count['year']
    df.at[i, 'month'] = count['month']


############################################
###   Write the partitioned time-series  ###
###   data to disk as parquet            ###
############################################

# https://github.com/pandas-dev/pandas/issues/27596#issuecomment-591026261

# Arrow is better than pandas
table = pyarrow.Table.from_pandas(df)


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

print(df.head())

# Works
pq.write_to_dataset(
    table,
    f"s3://{BUCKET}/{PREFIX}",
    partition_cols=["year", "month"],
    filesystem=fs,
    use_dictionary=True,
    compression="snappy",
    version="2.0"
)
