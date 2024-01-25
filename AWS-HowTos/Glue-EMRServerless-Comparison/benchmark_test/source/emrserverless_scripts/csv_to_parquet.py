from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import os
import sys

if len(sys.argv) < 4:
    print("Usage: script.py <s3_bucket_name> <input_path> <output_path>")
    sys.exit(-1)

# gets S3 bucket name from spark job params
s3_bucket_name = sys.argv[1]
input_path = sys.argv[2]
output_path = sys.argv[3]
print(f'Using s3 bucket name {s3_bucket_name}\n Reading from {input_path}\n Writing to {output_path}\n')

# Initialize Spark session
spark = SparkSession.builder.appName("CSV to Parquet").getOrCreate()

schema = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", IntegerType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType())
])

# Input and output paths
full_input_path = f"s3://{s3_bucket_name}/{input_path}"
full_output_path = f"s3://{s3_bucket_name}/{output_path}"

print(f'Reading CSV file from {full_input_path}')
# Reading the CSV file
df = spark.read.csv(full_input_path, header=True, schema=schema)

print(f'Writing data to {full_output_path}')
# Writing the data in Parquet format
df.write.mode("overwrite").parquet(full_output_path)

print('Done')