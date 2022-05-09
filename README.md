# Data Wrangler Merge

This application takes multiple .csv files located in AWS S3 and merges them into a dataset on AWS S3, as a single file on S3, or as a single file locally. 

```
usage: dw_merge.py [-h] [-b BUCKETPATH] [-r REMOTEWRITE] [-bf BIGFILE] [-l LOCALWRITE] [-c CHUNKSIZE] [-s DATASET] [-u USECOLS] [-d DELIMITER] [-n NA_REP] [-p CONCURRENT_PARTITIONING]

This application takes multiple .csv files located in AWS S3 and merges them into a dataset on AWS S3, as a single file on S3, or as a single file locally.

optional arguments:
  -h, --help            show this help message and exit
  -b BUCKETPATH, --bucketpath BUCKETPATH
                        S3 bucket with the path to .csv files. For example s3://remote-bucket/prefix/
  -r REMOTEWRITE, --remotewrite REMOTEWRITE
                        S3 bucket and path to write to. Note this uploads as datasets if bigfile is not specified. For example s3://remote-bucket/prefix/mydata
  -bf BIGFILE, --bigfile BIGFILE
                        Writes to S3 as one big file. Specify remotewrite with an explicit file name. For example s3://remote-bucket/prefix/big-file.csv
  -l LOCALWRITE, --localwrite LOCALWRITE
                        Local directory path and filename to write to.
  -c CHUNKSIZE, --chunksize CHUNKSIZE
                        If specified, return an generator where chunksize is the number of rows to include in each chunk. Default is 10000.
  -s DATASET, --dataset DATASET
                        If True read a CSV dataset instead of simple file(s) loading all the related partitions as columns. Default is False.
  -u USECOLS, --usecols USECOLS
                        If used return a subset of the columns.. For example -u firstName -u lastName -u dob
  -d DELIMITER, --delimiter DELIMITER
                        Delimiter used to separate values. Default is ,
  -n NA_REP, --na_rep NA_REP
                        Missing data representation.
  -p CONCURRENT_PARTITIONING, --concurrent_partitioning CONCURRENT_PARTITIONING
                        If True will increase the parallelism level during the partitions writing. It will decrease the writing time and increase the memory usage.
```

### Multi-part Data Set
The following will pull a directory of CSV files from S3 in a specified chunk `-c`. This will then upload the output to the specified directory as the designated data set. 

```
python3 ./dw_merge.py -b s3://spensireli-iot-artifacts/csv/ -r s3://spensireli-iot-artifacts/data-set/ -c 1000000 --dataset true
```

### Single Large CSV File
The following shows how you can pull multiple CSV files from AWS S3 and upload them back to S3 as a single file using the `bf True` flag. Note: This requires an equal amount of local disk
as being pulled remotely.

```
python3 ./dw_merge.py -b s3://spensireli-iot-artifacts/csv/ -r s3://spensireli-iot-artifacts/final-test.csv -bf True -c 1000000
```

### Local Large CSV File
The following shows how you can pull multiple CSV files from AWS S3 and store the contents locally as one single file. Note: This requires an equal amount of local disk as being pulled remotely. 

```
python3 ./dw_merge.py -b s3://spensireli-iot-artifacts/csv/ -l ./my-big-csv.csv -c 1000000
```

### Selecting Specific Columns
The use columns can be applied to any of the destinations. Below is an example of how to output only specific columns. 

```
python3 ./dw_merge.py -b s3://spensireli-iot-artifacts/poc/output-new/ -l ./my-big-csv.csv -u playerID -u birthYear -u birthMonth
```