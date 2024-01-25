import os
import json
import argparse
import boto3

import helpers.common as common
from helpers.cloudformation import get_stack_outputs
from helpers.emr_serverless import run_test as emr_serverless_run_test
from helpers.glue import run_test as glue_run_test

STACK_NAME = "cf-glue-vs-emr-serverless"
SCRIPT_FOLDER = os.path.dirname(os.path.abspath(__file__))

logger = common.get_logger()

emr_client = boto3.client("emr-serverless")
glue_client = boto3.client("glue")

#################################################################
outputs = get_stack_outputs(STACK_NAME)

s3_bucket = outputs["BenchmarkTestS3Bucket"]
# EMR
emrs_application_id = outputs["EMRServerlessApplicationId"]
emrs_execution_role_arn = outputs["EMRServerlessRoleArn"]
# Glue

#################################################################

# reads named arguments from command line (--test-file) [required]
parser = argparse.ArgumentParser()
parser.add_argument("--test-file", help="JSON test file name", required=True)
args = parser.parse_args()
test_file_name = args.test_file

# read test items from a file
test_items = None
with open(test_file_name,'r') as test_json_file: 
    # processing replacements
    test_items_str = test_json_file.read()
    test_items_str = test_items_str.replace("{s3_bucket}", s3_bucket)
    test_items = json.loads(test_items_str)

print(f"Test items: {test_items}")    

# for each test item, run the test
for test_item in test_items:    
    item_type = test_item["type"]
    repeat = int(test_item.get("repeat", "1"))
    for i in range(repeat):
        print(f"Run #{i+1} out of {repeat}")

        if item_type == "emr_serverless":
            # EMR Flow
            script_name = test_item["script_name"]
            local_script_fullpath = os.path.join(SCRIPT_FOLDER, script_name)
            arguments = test_item["arguments"]
            sparkSubmitParameters = test_item["sparkSubmitParameters"]
            run_name = test_item["run_name"]
            common.logger_add_file_handler(logger, run_name)
            logger.info(f"Processing test item: {json.dumps(test_item, indent=4, default=str)}")

            kwargs = {
                "emr_client": emr_client,
                "application_id": emrs_application_id,
                "execution_role_arn": emrs_execution_role_arn,
                "logger": logger,
                "run_name": run_name,
                "local_script_fullpath": local_script_fullpath,
                "s3_bucket": s3_bucket,
                "s3_script_path": script_name,
                "arguments": arguments,
                "sparkSubmitParameters": sparkSubmitParameters,
            }
            # this is optional - just to demonstrate how you can change compute capacity allocation for the job run
            # you also can leave it empty sparkSubmitParameters = {}
            # sparkSubmitParameters = {
            #     "spark.hadoop.hive.metastore.client.factory.class" : "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            #     "spark.executor.instances" : "1",
            #     "spark.dynamicAllocation.initialExecutors" : "1",
            #     "spark.dynamicAllocation.maxExecutors" : "1",
            #     "spark.executor.cores" : "2",
            #     "spark.executor.memory" : "8G",
            # }        

            emr_serverless_run_test(**kwargs)
            common.logger_remove_file_handler(logger, run_name)

        elif item_type == "glue":        
            script_name = test_item["script_name"]
            local_script_fullpath = os.path.join(SCRIPT_FOLDER, script_name)
            arguments = test_item.get("arguments",{})
            run_name = test_item["run_name"]
            common.logger_add_file_handler(logger, run_name)
            logger.info(f"Processing test item: {json.dumps(test_item, indent=4, default=str)}")
            kwargs = {
                "glue_client" : glue_client,
                "local_script_fullpath" : local_script_fullpath,
                "job_name" : test_item["job_name"],
                "arguments": arguments,
                "logger": logger,
            }

            glue_run_test(**kwargs)
            common.logger_remove_file_handler(logger, run_name)
            
        else:
            raise Exception(f"Unknown test item type: {item_type}")



