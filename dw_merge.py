"""
An application that uses awswrangler to merge
large CSV files. 
"""
import argparse
import awswrangler as wr
import sys
import logging
import pandas
import string
import os


parser = argparse.ArgumentParser(description='This application takes multiple .csv files located in AWS S3 and merges them into a dataset on AWS S3, as a single file on S3, or as a single file locally.')
parser.add_argument('-b', '--bucketpath', type=str, help='S3 bucket with the path to .csv files. For example s3://remote-bucket/prefix/')
parser.add_argument('-r', '--remotewrite', type=str, help='S3 bucket and path to write to. Note this uploads as datasets if bigfile is not specified. For example s3://remote-bucket/prefix/mydata')
parser.add_argument('-bf', '--bigfile', type=bool, default=False, help='Writes to S3 as one big file. Specify remotewrite with an explicit file name. For example s3://remote-bucket/prefix/big-file.csv')
parser.add_argument('-l', '--localwrite', type=str, help='Local directory path and filename to write to. ')
parser.add_argument('-c', '--chunksize', type=int, default=10000, help='If specified, return an generator where chunksize is the number of rows to include in each chunk. Default is 10000. ')
parser.add_argument('-s', '--dataset', type=bool, default=False, help='If True read a CSV dataset instead of simple file(s) loading all the related partitions as columns. Default is False.')
parser.add_argument('-u', '--usecols', action='append', help='If used return a subset of the columns.. For example -u firstName -u lastName -u dob')
parser.add_argument('-d', '--delimiter', type=str, default=',', help='Delimiter used to separate values. Default is ,')
parser.add_argument('-n', '--na_rep', type=str, default='NULL', help='Missing data representation.')
parser.add_argument('-p', '--concurrent_partitioning', type=bool, default=False, help='If True will increase the parallelism level during the partitions writing. It will decrease the writing time and increase the memory usage.')


# Not used, but included because Glue passes these arguments in
parser.add_argument('--extra-py-files', type=str, required=False, default=None, help="NOT USED")
parser.add_argument('--scriptLocation', type=str, required=False, default=None, help="NOT USED")
parser.add_argument('--job-bookmark-option', type=str, required=False, default=None, help="NOT USED")
parser.add_argument('--job-language', type=str, required=False, default=None, help="NOT USED")
parser.add_argument('--connection-names', type=str, required=False, default=None, help="NOT USED")



args = parser.parse_args()
bucket_path = args.bucketpath
remote_bucket = args.remotewrite
local_write = args.localwrite
chunk_size = args.chunksize
dataset = args.dataset
usecols = args.usecols
delimiter = args.delimiter
na_rep = args.na_rep
concurrent_partitioning = args.concurrent_partitioning
big_file = args.bigfile

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger(__name__)


def read_csv_data():
    if bucket_path:
        log.info("Chunksize Set to: %s" % chunk_size)
        log.info("Dataset Used: %s" % dataset)
        log.info('Reading data from: %s' % bucket_path)
        log.info('Usecols: %s' % usecols)

        total_records = 0
        empty_df = pandas.DataFrame()
        df = wr.s3.read_csv(bucket_path, chunksize=chunk_size, header=0, usecols=usecols, on_bad_lines='skip')
        for x in df:
            empty_df = pandas.concat([empty_df, x])
            rows = len(x.index)
            log.info('Found Rows: %s' % rows)
            total_records = total_records + rows
            log.info('Found Columns: %s' % len(x.columns))
        log.info('-----------------------')
        log.info('Total records processed: %s' % total_records)
        output_func(empty_df)

def output_func(df):
    if remote_bucket and big_file is False:
        wr.s3.to_csv(df, path=remote_bucket, mode='append', dataset=True, na_rep=na_rep, sep=delimiter, concurrent_partitioning=concurrent_partitioning)
        log.info('CSV is appending to %s' % remote_bucket)

    if local_write or big_file is True:
        if big_file:
            if remote_bucket is None:
                log.error("Missing a remote bucket.")
                return
            rando_string = string.ascii_uppercase
            file_name = f'./{rando_string}-temp.csv'
            df.to_csv(file_name, index=False, mode='a', header=False, na_rep=na_rep, sep=delimiter)
            log.info(f"Uploading data to s3 bucket: {remote_bucket}")
            log.info("The speed of the upload is dependent on your internet connection...")
            wr.s3.upload(local_file=file_name, path=remote_bucket)
            log.info(f"Removing temporary local file: {file_name}")
            os.remove(file_name)
            log.info(f"Upload to bucket {remote_bucket} complete!")
        else:
            df.to_csv(local_write, index=False, mode='a', header=False, na_rep=na_rep, sep=delimiter)
            log.info('CSV is appending to %s' % local_write)



def main():
    try:
        read_csv_data()
    except:
        log.error("Error running script")

if __name__ == "__main__":
    main()
