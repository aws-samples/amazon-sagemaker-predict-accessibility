import sys
from datetime import date
import requests
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, expr, when, round
from pyspark.sql.types import LongType

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

current_date=date.today()
path="year="+str(current_date.year)+"/month="+str(current_date.month)+"/day="+str(current_date.day)+"/"

playgrounds_raw_dir="s3://immersion-day-2.0-raw/playgrounds/" +path
playgrounds_url='https://www.nycgovparks.org/bigapps/DPR_Playgrounds_001.json'
playgroundsRDD = sc.parallelize([ requests.get(playgrounds_url).text])
playgrounds_df = spark.read.json(playgroundsRDD)
playgrounds_df.coalesce(1).write.format("parquet").mode("overwrite").save(playgrounds_raw_dir)

parks_raw_dir="s3://immersion-day-2.0-raw/parks/" +path
parks_url='https://www.nycgovparks.org/bigapps/DPR_Parks_001.json'
parksRDD = sc.parallelize([ requests.get(parks_url).text])
parks_df = spark.read.json(parksRDD)
parks_df.coalesce(1).write.format("parquet").mode("overwrite").save(parks_raw_dir)
