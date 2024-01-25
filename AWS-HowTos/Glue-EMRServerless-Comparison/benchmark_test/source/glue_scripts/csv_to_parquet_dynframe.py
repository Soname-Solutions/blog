import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions


print('Converts csv -> pq data. Using native AWS Glue Dynamic Frame functionality.')

# Fetching arguments
args = getResolvedOptions(sys.argv, ['s3_bucket_name','input_path','output_path'])
s3_bucket_name = args['s3_bucket_name']
input_path = args['input_path']
output_path = args['output_path']
print(f'Using s3 bucket name {s3_bucket_name}\n Reading from {input_path}\n Writing to {output_path}\n')

# Initializing Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Input and output paths
full_input_path = f"s3://{s3_bucket_name}/{input_path}"
full_output_path = f"s3://{s3_bucket_name}/{output_path}"

# Reading the CSV file into a DynamicFrame
dynamic_frame_read = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [full_input_path]},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="dynamic_frame_read"
)

# Define the mappings
mappings = [
    ("VendorID", "string", "VendorID", "int"),
    ("tpep_pickup_datetime", "string", "tpep_pickup_datetime", "timestamp"),
    ("tpep_dropoff_datetime", "string", "tpep_dropoff_datetime", "timestamp"),
    ("passenger_count", "string", "passenger_count", "int"),
    ("trip_distance", "string", "trip_distance", "double"),
    ("RatecodeID", "string", "RatecodeID", "int"),
    ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"),
    ("PULocationID", "string", "PULocationID", "int"),
    ("DOLocationID", "string", "DOLocationID", "int"),
    ("payment_type", "string", "payment_type", "int"),
    ("fare_amount", "string", "fare_amount", "double")
]

# Apply mappings
dynamic_frame_transformed = dynamic_frame_read.apply_mapping(mappings)

# Writing the data in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame = dynamic_frame_transformed,
    connection_type = "s3",
    connection_options = {"path": full_output_path},
    format = "parquet",
    transformation_ctx = "dynamic_frame_write"
)

print('Done')