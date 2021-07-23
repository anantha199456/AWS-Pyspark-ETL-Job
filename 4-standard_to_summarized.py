import configparser

import pandas as pd
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from writeto_outbound_archive import save_to_outbound_archive_loc
import boto3
import yaml

# Getting YAML file
from pyspark.sql.functions import col, when, to_date, weekofyear, year
from pyspark.sql.window import Window


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


def convert_parquet(ParquetDF, loc):
    ParquetDF.write.format("parquet").mode("overwrite").option("path", Stand_loc).save()


# Reading data from any required format
def read_data(spark_session, s3_file_path, fmt):
    vendor_df = spark_session.read.format(fmt).option('header', 'true').load(
        s3_file_path)
    return vendor_df


if __name__ == '__main__':
    # check counter file
    spark = SparkSession \
        .builder \
        .appName("DEMO_USECASE1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    # Creating config object to get the parameters
    config_file = get_spark_app_read_yaml('<enter-s3-bucketname>', 'anantha-subramanian/code/conf-s3.yaml')

    # Creating config object to get the parameters. if your using s3, you could use the below code.
    config_file = get_spark_app_read_yaml('config', 'conf-local.conf')

    outbound_key = config_file['outbound_key']
    archive_key = config_file['archive_key']
    standard_loc = config_file['standard_loc']

    # Summarized Layer starts from here.
    standard_parquet_df = read_data(spark, standard_loc, "parquet")
    # Converting th existing date filed to proper format so that it can be used for further processing
    standard_parquet_df = standard_parquet_df.withColumn('date_new',
                                                         when(col('Script_name') == 'INFY',
                                                              to_date(col('Date'), 'yyyy-MM-dd')).when(
                                                             col('Script_name') == 'SBIN',
                                                             to_date(col('Date'), 'dd-MM-yyyy')))
    standard_parquet_df = standard_parquet_df.withColumn("Week", weekofyear(col('date_new'))).withColumn("year", year(
        col('date_new')))

    # Creating Window spec objects for Open and Close
    windowSpec_open_col = (Window.partitionBy('Script_name', 'Week', 'year').orderBy(col("date_new").asc()))
    windowSpec_close_col = (Window.partitionBy('Script_name', 'Week', 'year').orderBy(col("date_new").desc()))
    df_ranked_open = standard_parquet_df.withColumn('rank', F.row_number().over(windowSpec_open_col))
    df_ranked_close = standard_parquet_df.withColumn('rank', F.row_number().over(windowSpec_close_col))

    df_open = df_ranked_open.filter(col('rank') == 1).select(df_ranked_open['Script_name'], df_ranked_open['Week'],
                                                             df_ranked_open['year'],
                                                             df_ranked_open['Date'].alias('Start_Date'),
                                                             df_ranked_open['Open'])
    df_close = df_ranked_close.filter(col('rank') == 1).select(df_ranked_close['Script_name'], df_ranked_close['Week'],
                                                               df_ranked_close['year'],
                                                               df_ranked_close['Date'].alias('End_Date'),
                                                               df_ranked_close['Close'])

    # Using join inner of the keys don;t match, then drop those sets
    op_close_df = df_open.join(df_close, [df_open.Week == df_close.Week, df_open.Script_name == df_close.Script_name,
                                          df_open.year == df_close.year], 'inner').drop(df_close['Script_name']).drop(
        df_close['Week']).drop(df_close['year'])

    # Calculating High Low and Volume
    Aggregated_df = standard_parquet_df.groupBy("Script_name", "Week", "year").agg(
        F.max(F.col("High")).alias("High_value"),
        F.min(F.col("Low")).alias("Low_value"),
        F.avg(F.col("Adj_Close")).alias(
            "Adj_Avg"),
        F.sum(F.col("Volume")).alias("Total_Volume"))

    # Getting summarized dataframe using aggregated and open_close df_close
    Summarized_df = op_close_df.join(Aggregated_df, [op_close_df.Week == Aggregated_df.Week,
                                                     op_close_df.Script_name == Aggregated_df.Script_name,
                                                     op_close_df.year == Aggregated_df.year], 'inner').drop(
        op_close_df['Script_name']).drop(op_close_df['Week']).drop(op_close_df['year'])
    Summarized_df = Summarized_df.sort(col('year'), col('Week'))

    Summarized_df.show(20)

    # Writing data from summarized dataframe to outbound s3 folder
    save_to_outbound_archive_loc(Summarized_df,outbound_key,archive_key)
