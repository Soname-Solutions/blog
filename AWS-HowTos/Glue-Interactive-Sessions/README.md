# How to accelerate your Glue Job development with Glue Interactive Sessions

In Glue Job development debugging part might become especially cumbersome.  
Every Glue Job execution might require you to wait for several minutes, only to potentially be confronted with an error message due to a simple oversight, like incorrect syntax.

There are several ways to speed up the process by partially developing code locally (for example using Docker container tailored for Glue).  
This article emphasizes another approach - using Glue interactive sessions. This method stands out for its ease of setup and its close replication of the environment and permissions encountered during actual Glue Job execution.

In this guide, we'll walk you through:
1. **Set Up** (a one-time configuration):
  - Setting up an IAM Role for Interactive Sessions (or, alternatively, re-using an IAM Role you plan to assign to your Glue Jobs)
  - Installing Jupyter (if it's not already on your local machine)
  - Installing necessary libraries and configuring your AWS CLI profile
2. **Debugging your Spark code within a Jupyter Notebook**, replicating step-by-step Glue Job execution.
3. **Converting** your Jupyter Notebook **into Glue Job script**.

Note: The use of Interactive sessions incurs charges based on the DPU (which relates to the number of workers) you use. The minimum requirement is 2 workers, equating to 2 DPUs, costing $0.80/hour in total (as per current pricing). While some may view this as a drawback, the substantial speed-up in development, in our perspective, more than compensates for the cost.

## Setting Up Interactive Sessions

1. Create a Python virtual environment and install required libraries:
```shell
python -m venv jupyterenv
call jupyterenv/scripts/activate

pip3 install --upgrade jupyter boto3 aws-glue-sessions

install-glue-kernels
```

2. Here we create an IAM role which will be used by Jupyter Notebook to run commands:
```shell
aws cloudformation deploy --template-file cloudformation/gluerole.yaml --stack-name cf-glueintsessiondemo-role --capabilities CAPABILITY_NAMED_IAM
```

Make a note of the IAM Role's ARN found in the CloudFormation stack's outputs, as we'll be using it to initialize our Jupyter session.

This IAM Role contains:
- Managed Service policy for Glue Interactive Sessions
- Additional policies your Glue Job might need. For example, if you you need to read/write S3 objects - it would require S3 permissions policy attached to the role. For demo purposes we use AmazonS3FullAccess policy here instead of fine-grained one.

3. Configure you AWS credentials

We will run Interactive sessions on behalf of a IAM user.
You can create a new user or reuse an existing one, but it's required to:
- Have Access keys generated for this user
- Glue Interactive Session privileges which include:
  - AWSGlueConsoleFullAccess managed policy
  - policy allowing iam:PassRole on a role created before

Sample JSON definition for the policy:
```json
        {
            "Sid": "passrole",
            "Effect": "Allow",
            "Action": [
                "iam:PassRole"
            ],
            "Resource": "arn:aws:iam::<<your account id>>:role/iamr-glueintsessionsdemo"
        }
```

Make sure you have a profile for this user configured in your <HOME_DIR>/.aws/credentials file.

That's how credentials could look like in .credentials file (replace with your values)
```ini
[profile_for_article]
region=<put your region here>
aws_access_key_id=<put your access key here>
aws_secret_access_key=<put your secret access key here>
```

From this point onward, we'll utilize the profile named "profile_for_article".

## Launching the Jupyter Notebook and debugging your code

With our setup complete, it's time to dive into the development of our Glue Spark code.

### Sample Glue Job idea

Here's a brief overview of the intended Glue Job functionality:
- Get S3 bucket name (containing input and output data) as a parameter
- Read CSV data from S3 (we'll use a sample csv file containing data about 50 lakes on Earth)
- Process this data by converting it from CSV to Parquet format, and then saving it in an S3 output folder
- Aggregate the data to calculate the number of lakes per continent
- Store this aggregated information as an Iceberg table and also display the content of the dataframe

In this demo we'll take a look at a few additional aspects:
- How to emulate parameters passed to a Glue Job.
- How to set up interactive session (and, eventually, Glue Job itself) to work with Open Table Formats (such as Iceberg).

Code is available in glueintsessionsdemo.ipynb notebook file and we'll guide you through the steps in the next section.

We need to prepare sample data onto S3:
- First, create an S3 bucket (we use "s3-glueintsessionsdemo-data" in this article, while you'll need to select a unique name for your own bucket).
- Next, upload lakes.csv file, which can be found in the "sample_data" directory on Git, into the "in/lakes/" path on S3.

### Let's implement it
 
1. Let's start a new Jupyter Notebook:

- Run command
```shell
jupyter notebook
```
- Click New -> Notebook
- Choose "Glue PySpark" as the Kernel

2. In the notebook let's do some initialization:

(replace with your values)
```
%profile profile_for_article
%iam_role <put your IAM role here>
%idle_timeout 10
%number_of_workers 2
```

**%idle_timeout** - The number of minutes of inactivity after which session is stopped. A nice feature to ensure you don't inadvertently inflate your AWS bill due to forgotten active sessions.  
**%number_of_workers** - Lets you specify the desired number of workers (while 2 is minimum). It depends on the processing power you need. Here we
keep it at the bare minimum for demo purposes.

3. Next we need to preconfigure our session:

The following %%configure magic simulates a Glue Job parameter:
```python
%%configure
{
    "--s3_bucket_name" : "s3-glueintsessionsdemo-data"
}
```

In the next cell, we're adding a few more settings. If this were a real Glue Job, these would also need to be passed as parameters. These particular settings enable the use of the Iceberg format. Remember to swap out the S3 bucket name with your own!

```python
%%configure
{
    "--datalake-formats" : "iceberg",
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://s3-glueintsessionsdemo-data/iceberg/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
}
```

4. Now let's start our session:
```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
```
This code is identical to what we have in the beginning of our typical Glue Job.
It will take some time (around 40 seconds) to get ready and we can start developing our code!

Note: Session creation is the moment when AWS billing starts.

5. Spark code:

Now let's add some code cells which implement intended behavior.

a. **Read the parameter value**:

```python
from awsglue.utils import getResolvedOptions
parameter_name = 's3_bucket_name'
args = getResolvedOptions(sys.argv,[parameter_name])
s3_bucket_name = args[parameter_name]
```

b. **Read data**:
```python
# 1. Read dataframe directly from s3 object using the variable
df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"s3://{s3_bucket_name}/in/lakes/")
```

c. **Write Parquet**:
```python
# 2. Convert it into parquet format and writes to specified S3 path using the variable
df.write.mode("overwrite").parquet(f"s3://{s3_bucket_name}/out/lakes/")
```

d. **Aggregate and output**:
```python
# 3. Prepare aggregated dataframe
agg_df = df.groupBy("continent").count().withColumnRenamed("count", "number_of_lakes")

# Outputs dataframe
agg_df.show()
```

e. **Write aggregated data into an Iceberg table**:
```python
# 4. Write aggregated dataframe to Iceberg table
agg_df.createOrReplaceTempView("tmp_lakes")

query = f"""
CREATE TABLE IF NOT EXISTS glue_catalog.default.lakes_iceberg
USING iceberg
AS SELECT * FROM tmp_lakes
"""
spark.sql(query)
```
Note: Now you can also select data from this table using Athena.


**Stop Interactive session**:

Once you finished debugging your code, it's a good idea to end the interactive session.
Sure, it'll time out on its own based on the settings we set up at the start. But, for peace of mind and to avoid any extra costs, it's best to shut it down manually.

```python
%stop_session
```


Here we have developed a really simple Glue Job. The benefits here was that even when we did something wrong we didn't have to re-run the whole Glue Job and wait till it's gets initialized to execute code to the failing point. We are able just to fix and re-run failing piece of code.

With our code now ready, let's move on to converting it into an actual Glue Job script.

## Converting the Notebook into Glue Job script

"nbconvert" is the tool you can use to convert your notebook file into a python script for a glue job.

1. Installing nbconvert:
```shell
pip install nbconvert
```

2. Using nbconvert CLI to convert:
```shell
jupyter nbconvert --to script glueintsessionsdemo.ipynb
```
Then we just need to remove Jupyter Magics and your script is ready!

## Clean Up

1. Empty S3 bucket and delete it
2. Delete CloudFormation template

```shell
aws cloudformation delete-stack --stack-name cf-glueintsessiondemo-role
```

3. Delete table from Glue Catalog

In AWS Athena query editor execute:
```sql
DROP TABLE lakes_iceberg;
```
