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
parser.add_argument('-r', '--remotewrite', type=str, help='S3 bucket and path to write to. Note this uploads as datasets. For example s3://remote-bucket/prefix/')
parser.add_argument('-l', '--localwrite', type=str, help='Local directory path and filename to write to. ')
parser.add_argument('-c', '--chunksize', type=int, default=10000, help='If specified, return an generator where chunksize is the number of rows to include in each chunk. Default is 100. ')
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

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger(__name__)


def read_csv_data():
    if bucket_path:
        log.info("Chunksize Set to: %s" % chunk_size)
        log.info("Dataset Used: %s" % dataset)
        log.info('Reading data from: %s' % bucket_path)
        log.info('Usecols: %s' % usecols)

        total_records = 0

        df = wr.s3.read_csv(bucket_path, chunksize=chunk_size,header=0, usecols=usecols, on_bad_lines='skip')
        for x in df:
            rows = len(x.index)
            log.info('Found Rows: %s' % rows)
            total_records = total_records + rows
            log.info('Found Columns: %s' % len(x.columns))
            output_func(x)
        log.info('-----------------------')
        log.info('Total records processed: %s' % total_records)

def output_func(df):
    if remote_bucket:
        wr.s3.to_csv(df, path=remote_bucket, mode='append', dataset=True, na_rep=na_rep, sep=delimiter, concurrent_partitioning=concurrent_partitioning)
        log.info('CSV is appending to %s' % remote_bucket)

    if local_write:
        df.to_csv(local_write, index=False, mode='a', header=False, na_rep=na_rep, sep=delimiter)
        log.info('CSV is appending to %s' % local_write)

def main():
    try:
        read_csv_data()
    except:
        print("Error running script")

if __name__ == "__main__":
    main()
