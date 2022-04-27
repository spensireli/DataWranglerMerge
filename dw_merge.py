"""
An application that uses awswrangler to merge
large CSV files. 
"""
import argparse
import awswrangler as wr
import sys
import logging

parser = argparse.ArgumentParser(description='Uses awswrangler to join csv files located in s3.')
parser.add_argument('-b', '--bucketpath', type=str, help='S3 bucket with the path to .csv files. For example s3://remote-bucket/prefix/')
parser.add_argument('-r', '--remotewrite', type=str, help='S3 bucket and file name to write to. For example s3://remote-bucket/prefix/')
parser.add_argument('-l', '--localwrite', type=str, help='Local directory path and filename to write to. ')
parser.add_argument('-c', '--chunksize', type=int, default=100, help='If specified, return an generator where chunksize is the number of rows to include in each chunk. Default is 100. ')
parser.add_argument('-s', '--dataset', type=bool, default=False, help='If True read a CSV dataset instead of simple file(s) loading all the related partitions as columns. Default is False.')

args = parser.parse_args()
bucket_path = args.bucketpath
remote_bucket = args.remotewrite
local_write = args.localwrite
chunk_size = args.chunksize
dataset = args.dataset


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger(__name__)


def read_csv_data():
    if bucket_path:
        log.info("Chunksize Set to: %s" % chunk_size)
        log.info("Dataset Used: %s" % dataset)
        log.info('Reading data from %s' % bucket_path)
        # Looks like chunking is returned and needs to be itnerated upon.
        # df = wr.s3.read_csv(bucket_path, chunksize=chunk_size)
        df = wr.s3.read_csv(bucket_path)
        return df

def output_func(df):
    print(df)
    print(remote_bucket)
    print(local_write)
    if remote_bucket:
        wr.s3.to_csv(df, path=remote_bucket)
        log.info('CSV is now available at %s' % remote_bucket)

    if local_write:
        # df.to_csv(local_write, index=False, chunksize=chunk_size)
        df.to_csv(local_write, index=False)
        log.info('CSV is now available at %s' % local_write)

def main():
    try:
        df = read_csv_data()
        output_func(df)
    except:
        print("Error running script")

if __name__ == "__main__":
    main()
