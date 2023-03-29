import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

query = "SELECT 1 as id  UNION ALL SELECT 2 as id"
connection_name = "glueconn-awshowto-sap"
connection_type = "custom.jdbc"
connection_options = { "query": query, "connectionName": connection_name}

dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type=connection_type,
        connection_options=connection_options,
        transformation_ctx="dynamic_frame"
)

dynamic_frame.show()
