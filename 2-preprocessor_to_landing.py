import configparser
import json
import os

import pandas as pd
from pyspark import SparkConf
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


def check_file_format(loc_vendor):
    for loc_ in loc_vendor:
        name, extension = os.path.splitext(loc_)
        # print(loc)
        boolean = True
        if extension != ".csv":
            read_file = pd.DataFrame(pd.read_excel(loc_))
            read_file.to_csv(name + ".csv", sep=',', index=False, header=True, quotechar='\"', )
            print("Wrong Format, Converted to CSV for :" + loc_)
        else:
            print("Already Converted,In CSV Format" + loc_)
    return boolean


def get_counter(bucket, counter_file_path):
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(bucket)
    content_object = s3.Object(bucket, counter_file_path)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content


def counter_file_check(df1, df2, bucket_n, counter_file_infy, counter_path_sbin):
    boolean = True
    json_infy = get_counter(bucket_n, counter_file_infy)
    json_sbin = get_counter(bucket_n, counter_path_sbin)
    if df1 != json_infy:
        boolean = False
        print("Count Check Failed for INFY")
    elif df2 != json_sbin:
        boolean = False
        print("Count Check Failed for SBIN")

    return boolean


def get_size(preprocess_bucket, prefix):
    size = 0
    s3 = boto3.resource('s3')
    src_bucket = s3.Bucket(preprocess_bucket)
    for file in src_bucket.objects.filter(Prefix=prefix):
        if file.key != "indicator.txt":
            size = size + file.size
    return round(size / 1024)


def indicator_check(bucket, prefix, indicator_key):
    total_file_size = get_size(bucket, prefix)
    print("total_file_size : " + str(total_file_size))
    s3 = boto3.client('s3')
    res = s3.get_object(Bucket=bucket, Key=indicator_key)
    data = res['Body'].read()
    summary_size = int(str(data.split('SUMMARY')[1].strip().split(' ')[0]))
    # print("summary_size : " + summary_size)
    if total_file_size == summary_size:

        return True
    else:

        return False


def check_header_validation(sparkSession, file_location):
    # df = pd.read_csv(file_location)
    df = sparkSession.read.format("csv").option('header', 'true').load(file_location)
    df1 = df.withColumnRenamed("Scrip name", "Script name")
    data_top = list(df1.columns)
    # print(data_top)
    return data_top


def read_count_data(loc):
    return spark.read.format('csv').option('header', 'false').load(loc).count()


if __name__ == '__main__':
    # check counter file
    spark = SparkSession \
        .builder \
        .appName("Demo-2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Creating config object to get the parameters
    config_file = get_spark_app_read_yaml('<enter-s3-bucketname>', 'anantha-subramanian/code/conf-s3.yaml')

    # Creating config object to get the parameters. if your using s3, you could use the below code.
    config_file = get_spark_app_read_yaml('config', 'conf-local.conf')

    # getting all the configurations from the yaml file
    sbin_loc_inbound = config_file['sbin_loc_inbound']
    infy_loc_inbound = config_file['infy_loc_inbound']
    s3_landing_infy = config_file['s3_landing_infy']
    sbin_loc_csv = config_file['sbin_loc_csv']
    infy_loc_csv = config_file['infy_loc_csv']
    bucket_name = config_file['bucket_name']
    counter_file_path_infy = config_file['counter_file_path_infy']
    counter_file_path_sbin = config_file['counter_file_path_sbin']
    preprocessor_bucket_key = config_file['preprocessor_bucket_key']
    indicator_key_infy = config_file['indicator_key_infy']
    indicator_key_sbin = config_file['indicator_key_sbin']
    schema_file = config_file['schema_file']

    # File format check. If its not in csv format, convert it.
    location = [sbin_loc_inbound, infy_loc_inbound]
    if check_file_format(location):
        print("File Format Validation Cleared")

    # Counter File check
    sbin_df_count = read_count_data(sbin_loc_csv)
    infy_df_count = read_count_data(infy_loc_csv)
    # code to check if the count matches with the configuration given in the pre-processor folder
    if counter_file_check(sbin_df_count, infy_df_count, bucket_name, counter_file_path_infy, counter_file_path_sbin):
        print("All the validation passed")
    else:
        print("Counter Check Failed, Please check")

    # indicator file check
    indicator_check_infy = indicator_check(bucket_name, preprocessor_bucket_key + "INFY/", indicator_key_infy)
    indicator_check_sbin = indicator_check(bucket_name, preprocessor_bucket_key + "SBIN/", indicator_key_sbin)
    # if indicator_check_infy and indicator_check_sbin:
    if indicator_check_infy == True and indicator_check_sbin == True:
        print("Indicator file check completed with success")
    else:
        print("File size not matching, please check!")

    # schema header validation check
    location_csv = [infy_loc_csv, sbin_loc_csv]
    rddFromFile = spark.sparkContext.textFile(schema_file)
    # colsList = rddFromFile.collect()
    colsList = ['Script name', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    for loc in location_csv:
        header_list = check_header_validation(spark, loc)
        if header_list == colsList:
            print("Schema Validation Passed for :" + loc)
        else:
            print("Issue with header format for :" + loc)
