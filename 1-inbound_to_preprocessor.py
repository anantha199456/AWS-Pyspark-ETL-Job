import configparser

from pyspark import SparkConf
from pyspark.sql import SparkSession
import boto3
import yaml


def copy_to_s3_location(source_bucket, source_key, dest_bucket, des_key):
    s3 = boto3.resource('s3')
    copy_source = {
        'Bucket': source_bucket,
        'Key': source_key
    }
    s3.meta.client.copy(copy_source, dest_bucket, des_key)


# Getting YAML file
def get_spark_app_read_yaml(bucket, key):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    configfile = yaml.safe_load(response["Body"])
    return configfile


# you can use configParser if you are using it locally.
def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("config/config-local.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


if __name__ == '__main__':
    # check counter file
    spark = SparkSession \
        .builder \
        .appName("Demo-1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Creating config object to get the parameters. if your using s3, you could use the below code.
    config_file = get_spark_app_read_yaml('config', 'conf-local.conf')

    config_file_local = get_spark_app_config()
    #
    # getting all the configurations from the yaml file
    bucket_name = config_file_local['bucket_name']
    inbount_bucket_key = config_file_local['inbount_bucket_key']
    preprocessed_sbin_bucket_key = config_file_local['preprocessed_sbin_bucket_key']
    preprocessed_infy_bucket_key = config_file_local['preprocessed_infy_bucket_key']

    # copy inbound to preprocessed folders
    copy_to_s3_location(bucket_name, inbount_bucket_key + "INFY/INFY.xls", bucket_name,
                        preprocessed_infy_bucket_key)
    copy_to_s3_location(bucket_name, inbount_bucket_key + "SBIN/SBIN.csv", bucket_name,
                        preprocessed_sbin_bucket_key)
