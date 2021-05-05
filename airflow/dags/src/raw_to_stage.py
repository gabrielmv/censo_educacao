from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
from os import environ as env
import json

spark = SparkSession.builder.appName("raw_to_stage").getOrCreate()

parameters = json.loads(env["PARAMETERS"])

source_path = parameters["source_path"]
target_path = parameters["target_path"]
mode = parameters["mode"]

def pandas_to_spark(spark, pandas_df):
    pandas_df.columns = pandas_df.columns.str.strip()     
    pandas_df.columns = pandas_df.columns.str.replace(' ', '_')         
    pandas_df.columns = pandas_df.columns.str.replace(r"[^a-zA-Z\d\_]+", "") 

    fields = []
    for column in list(pandas_df.columns): 
        fields.append(StructField(column, StringType()))
    schema = StructType(fields)
    
    return spark.createDataFrame(pandas_df, schema)

if mode == "xlsx":
    sheet_name = parameters["sheet_name"]
    skiprows = int(parameters["skiprows"])

    pandas_df = pd.read_excel(source_path, sheet_name=sheet_name, skiprows=skiprows, skipfooter=1,engine='openpyxl')
    df = pandas_to_spark(spark, pandas_df)
else:
    df = spark.read.option("header",True).options(delimiter='|').csv(source_path)
    
df.write.mode("overwrite").parquet(target_path)
