import sys
import boto3
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


path="income-ny/year=2016/income.csv"
raw_bucket='immersion-day-2.0-raw'
s3 = boto3.resource('s3')
bucket = s3.Bucket(raw_bucket)

objs = list(bucket.objects.filter(Prefix=path))
if len(objs) > 0 and objs[0].key == path:
 print("Object "+path+" already exists!")
else:
 print("Starting download")
 income_url='https://www.irs.gov/pub/irs-soi/16zpallagi.csv'
 income_csv=requests.get(income_url).text
 # Method 1: Object.put()
 object = s3.Object(raw_bucket, path)
 object.put(Body=income_csv)
 print("Done")
