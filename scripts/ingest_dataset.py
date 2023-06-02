from pyspark.sql import functions as F, SparkSession
import boto3
from process import process
from utils import  get_config_values, s3_util, parse_schema_yaml
from datetime import datetime
import argparse
import logging 

parser = argparse.ArgumentParser()
parser.add_argument('--dataset_name', help='the name of the data set')
logger = logging.getLogger()

if __name__ == '__main__':
    #we can apply custome config based one the data size
    args = parser.parse_args()
    dataset= args.dataset_name
    tables_schema= parse_schema_yaml()
    try:
        table_schema = tables_schema[dataset]
    except KeyError:
        logger.error("the data set in not registered in tables schema yaml", exc_info=True)
        raise

    spark = (
    SparkSession.builder
    .appName('BILLING_{dataset}')
    .enableHiveSupport()
    .config("spark.executor.cores", 2)
    .config("hive.exec.dynamic.partition.mode","nonstrict")
    .config("spark.sql.legacy.timeParserPolicy","LEGACY")
    .config("spark.sql.autoBroadcastJoinThreshold", 104857600)
    .getOrCreate()
    )

    #get data set config properties
    config = get_config_values()
    target_path = config['target_path']
    sources =  config['sources']
    source_path = sources[dataset]['path']
    target_schema = config['target_schema']
    # run time stamp
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    merge_key = sources[dataset]['merge_key']
    cdc_key = sources[dataset]['cdc_key']
    s3_bucket = config['s3_bucket']
    source_prefix = f"source/{dataset}"
    target_prefix =f"archive/{dataset}"
    # file name for lineage purpose
    file_name=""
    try:
        file_name = s3_util (boto3,s3_bucket, source_prefix,ts, getfilename=True)
    except Exception:
        logger.info(f"file for {dataset} is not recieved on {ts} ", exc_info=True)
    #start processing
    process( spark,source_path,target_path,target_schema, dataset, table_schema, merge_key, cdc_key, ts, file_name)
   
    # archive processed files
    try:
        s3_util (boto3,s3_bucket, source_prefix,ts, target_prefix=target_prefix, archivefile=True)
    except Exception:
        logger.info(f"not able to archive {dataset} on {ts}. Please move the files manually to avoid duplication ", exc_info=True)
    