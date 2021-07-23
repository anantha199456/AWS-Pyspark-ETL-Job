import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import boto3
import yaml

# Getting YAML file
from pyspark.sql.functions import col, when, to_date, weekofyear, year
from pyspark.sql.window import Window


def save_to_outbound_archive_loc(Summarized_df, outbound_key, archive_key):
    Summarized_df.coalesce(1).write.format('csv').mode('overwrite').save(outbound_key, header='true')
    # Saving data to Archive s3 folder - Last Seetp
    Summarized_df.coalesce(1).write.format('csv').mode('overwrite').save(archive_key, header='true')
