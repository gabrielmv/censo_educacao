from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
from os import environ as env

spark = SparkSession.builder.appName("curated_to_dw").getOrCreate()

parameters = json.loads(env["PARAMETERS"])

source_path = parameters["source_path"]
database_url = parameters["database_url"]
table = parameters["table"]
user = parameters["user"]
password = parameters["password"]


df = spark.read.parquet(source_path)

properties = {"user": user,"password": password,"driver": "org.postgresql.Driver"}
df.write.jdbc(url=database_url, table=table, mode="overwrite", properties=properties)
