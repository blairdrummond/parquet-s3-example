# Multi-file Parquet datasets

**NOTE: pyarrow>=3.0 is a hard requirement, as it fixes an s3fs bug. See [ARROW-10546](https://issues.apache.org/jira/browse/ARROW-10546)**

Parquet supports this out of the box

https://arrow.apache.org/docs/python/parquet.html#partitioned-datasets-multiple-files

Note, final size in minio is 1mb, instead of the 4mb csv. This is because of parquet + snappy compression.

# Automatic partitioning and organization

```
# mc tree localminio/
localminio
└─ cars
   └─ raw
      ├─ year=2015
      │  ├─ month=10
      │  ├─ month=11
      │  ├─ month=12
      │  ├─ month=5
      │  ├─ month=6
      │  ├─ month=7
      │  ├─ month=8
      │  └─ month=9
      ├─ year=2016
      │  ├─ month=1
      │  ├─ month=10
      │  ├─ month=11
      .
      .
      │  └─ month=9
      ├─ year=2017
      │  ├─ month=1
      │  ├─ month=10
      │  ├─ month=11
      .
      .
      │  └─ month=9
      ├─ year=2018
      │  ├─ month=1
      .
      .
      │  └─ month=9
      ├─ year=2019
      │  ├─ month=1
      .
      .
      │  └─ month=9
      └─ year=2020
         ├─ month=1
      .
      .
         └─ month=9
```



# Learn more

Apparently Arrow is upgrading its DataSet metadata API to allow for fancier filtering techniques; might be worth looking into.
