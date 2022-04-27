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
parser.add_argument('-c', '--chunksize', type=int, default=10000, help='If specified, return an generator where chunksize is the number of rows to include in each chunk. Default is 100. ')
parser.add_argument('-s', '--dataset', type=bool, default=False, help='If True read a CSV dataset instead of simple file(s) loading all the related partitions as columns. Default is False.')

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


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger(__name__)


def read_csv_data():
    if bucket_path:
        log.info("Chunksize Set to: %s" % chunk_size)
        log.info("Dataset Used: %s" % dataset)
        log.info('Reading data from %s' % bucket_path)
        df = wr.s3.read_csv(bucket_path, chunksize=chunk_size, header=0)
        for x in df:
            output_func(x)

def output_func(df):
    if remote_bucket:
        wr.s3.to_csv(df, mode='a', path=remote_bucket)
        log.info('CSV is appending to %s' % remote_bucket)

    if local_write:
        df.to_csv(local_write, index=False, mode='a', header=False)
        log.info('CSV is appending to %s' % local_write)

def main():
    try:
        read_csv_data()
    except:
        print("Error running script")

if __name__ == "__main__":
    main()
