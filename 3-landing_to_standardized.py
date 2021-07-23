import configparser

import pandas as pd
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import boto3
import yaml


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


def convert_parquet(ParquetDF, Stand_loc):
    ParquetDF.write.format("parquet").mode("overwrite").partitionBy("Script_name").option("path", Stand_loc).save()


def standardized_layer_partition(loc_list, standardized_bucket_infy, standardized_bucket_sbin):
    for vendor_loc in loc_list:
        if "INFY" in vendor_loc:
            vendorINFYParquetDF = read_data(spark, vendor_loc, 'csv')
            convert_parquet(vendorINFYParquetDF, standardized_bucket_infy)
        else:
            vendorSBINParquetDF = read_data(spark, vendor_loc, 'csv')
            convert_parquet(vendorSBINParquetDF, standardized_bucket_sbin)


# Reading data from any required format
def read_data(spark_session, s3_file_path, fmt):
    vendor_df = spark_session.read.format(fmt).option('header', 'true').load(
        s3_file_path)
    return vendor_df


if __name__ == '__main__':
    # check counter file
    spark = SparkSession \
        .builder \
        .appName("1st_Step_UseCase1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Creating config object to get the parameters
    config_file = get_spark_app_read_yaml('<enter-s3-bucketname>', 'anantha-subramanian/code/conf-s3.yaml')

    # Creating config object to get the parameters. if your using s3, you could use the below code.
    config_file = get_spark_app_read_yaml('config', 'conf-local.conf')

    # getting all the configurations from the yaml file
    sbin_loc_inbound = config_file['sbin_loc_inbound']
    s3_landing_infy = config_file['s3_landing_infy']
    s3_landing_sbin = config_file['s3_landing_sbin']
    landing_infy_loc = config_file['landing_infy_loc']
    landing_sbin_loc = config_file['landing_sbin_loc']
    standardized_bucket_key_infy = config_file['standardized_bucket_key_infy']
    standardized_bucket_key_sbin = config_file['standardized_bucket_key_sbin']

    # Making some modifications to landing folder with few modifications to column name before
    # sending it to Standardized directory
    source_data = read_data(spark, s3_landing_infy, "csv")
    source_data1 = read_data(spark, s3_landing_sbin, "csv")
    final_infy_data = (source_data.withColumnRenamed('Script name', 'Script_Name')
                       .withColumnRenamed('Adj Close', 'Adj_Close'))
    final_sbin_data = (source_data1.withColumnRenamed('Scrip name', 'Script_Name')
                       .withColumnRenamed('Adj Close', 'Adj_Close'))

    final_infy_data.write.format("csv").mode("append").option("path", s3_landing_infy).option("header",
                                                                                              'True').save()
    final_sbin_data.write.format("csv").mode("append").option("path", s3_landing_sbin).option("header",
                                                                                              'True').save()

    # convert the csv files to parquet files in Standardized layer (Partition).
    location_new = [landing_infy_loc, landing_sbin_loc]
    standardized_layer_partition(location_new, standardized_bucket_key_infy, standardized_bucket_key_sbin)
    print("converted the csv files to parquet files in Standardized layer (Partition)")
