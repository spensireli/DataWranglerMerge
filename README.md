# Data Wrangler Merge

This application takes multiple .csv files located in AWS S3 and merges them into a single file. The output of this file can be local or remote.

```
usage: dw_merge.py [-h] [-b BUCKETPATH] [-r REMOTEWRITE] [-l LOCALWRITE] [-c CHUNKSIZE] [-s DATASET]

Uses awswrangler to join csv files located in s3.

optional arguments:
  -h, --help            show this help message and exit
  -b BUCKETPATH, --bucketpath BUCKETPATH
                        S3 bucket with the path to .csv files. For example s3://remote-bucket/prefix/
  -r REMOTEWRITE, --remotewrite REMOTEWRITE
                        S3 bucket and file name to write to. For example s3://remote-bucket/prefix/
  -l LOCALWRITE, --localwrite LOCALWRITE
                        Local directory path and filename to write to.
  -c CHUNKSIZE, --chunksize CHUNKSIZE
                        If specified, return an generator where chunksize is the number of rows to include in each chunk. Default is 100.
  -s DATASET, --dataset DATASET
                        If True read a CSV dataset instead of simple file(s) loading all the related partitions as columns. Default is False.
```