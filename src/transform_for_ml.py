import sys
from datetime import date
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import when
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, expr, when, round
from pyspark.sql.types import LongType


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

today = date.today()


current_date=date.today()
path="year="+str(today.year)+"/month="+str(today.month)+"/day="+str(today.day)+"/"
processed_dir="s3://immersion-day-2.0-processed/processed/"+path
partition_predicate="(year=='"+str(today.year)+"' and month=='"+str(today.month)+"' and day=='"+str(today.day)+"')"

# Create a DynamicFrame using the 'parks' table
parks_DyF = glueContext.create_dynamic_frame.from_catalog(database="immersion-day-2.0-raw", table_name="parks", push_down_predicate = partition_predicate)
# Create a DynamicFrame using the 'playgrounds' table
playgrounds_DyF = glueContext.create_dynamic_frame.from_catalog(database="immersion-day-2.0-raw", table_name="playgrounds", push_down_predicate = partition_predicate)
# Create a DynamicFrame using the 'income_ny' table
income_DyF = glueContext.create_dynamic_frame.from_catalog(database="immersion-day-2.0-raw", table_name="income_ny",push_down_predicate="(year=='2016')")
income_ny_DyF = Filter.apply(frame = income_DyF,
                    f = lambda x: x["state"] in ["NY"] )
print("Filtered income count:  ", income_ny_DyF.count())
income_ny_DyF.printSchema()

# Print out information about this data
print("Parks Count:  ", parks_DyF.count())
parks_DyF.printSchema()

# Print out information about this data.
print("Playground Count:  ", playgrounds_DyF.count())
playgrounds_DyF.printSchema()
# Convert to Spark DataFrame for left outer join
playgrounds_df=playgrounds_DyF.toDF()
# Drop duplicate columns in parks dataframe
columns_to_drop=['Location', 'Name','year','month','day']
playgrounds_df=playgrounds_df.drop(*columns_to_drop)
#Rename duplicate/join column to drop it later after join
playgrounds_df=playgrounds_df.withColumnRenamed("Prop_ID", "Playground_Prop_ID")




parks_income_ny_DyF = Join.apply(income_ny_DyF,parks_DyF,"zipcode","Zip")
print("joined table count:  ", parks_income_ny_DyF.count())
parks_income_ny_DyF.printSchema()
# Convert to spark data frame for left outer join
parks_income_ny_df= parks_income_ny_DyF.toDF()

columns_to_drop=['Zip','Playground_Prop_ID','year','month','day']
final_output =  playgrounds_df.join(parks_income_ny_df, playgrounds_df.Playground_Prop_ID == parks_income_ny_df.Prop_ID,how='left_outer').drop(*columns_to_drop)
print("Final Count:  ", final_output.count())
final_output.printSchema()
final_output.show(1)
print("Writing joined file to processed")
final_output.coalesce(1).write.format("parquet").mode("overwrite").save(processed_dir)
