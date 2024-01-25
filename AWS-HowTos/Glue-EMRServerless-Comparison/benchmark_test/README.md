# Benchmark Tests for AWS Glue and EMR Serverless Jobs

## Test objectives:

We wanted to simulate one of typical Big Data Platform workloads, run it in AWS Glue and EMR Serverless.  
Measurement would be:
1. Cost to process data ($)
2. Time required to process data (and, also, time to spin up required resources in order to asses total test run-time)

## Test scenario:

We simulated a common batch processing step: loading data from "raw" to "stage" layer.  
The test involved processing a ~4GB CSV dataset of New York taxi (yellow taxi) ride data from 2022, stored on S3.  
This dataset size is in typical range for incremental data batches (which is usually up to several GBs).  

Workload includes:
- reading csv data from S3
- data type conversion (all csv string columns to actual data format)
- writing data to a different S3 location in parquet format.

Note that this test represents a specific data processing scenario and is not intended as a comprehensive evaluation.

## Repository structure

1. cloudformation - contains yaml template to spin up resources for test (including S3 bucket for scripts and data, IAM roles for Glue and EMR-Serverless, Glue Jobs and EMR Application)
2. log_results - saved logs from test runs (1 log per each setup, inside each log - information about 3 runs). note: _dynframe_ lgs has just 1 result, as it was comparison, what to use in Glue Jobs: Dynamic Frame or spark native data frame.
3. source - test runner source code and auxiliary libraries
4. test - json definition for tests (telling to python test runner scripts - what to run and in which set up)
5. **benchmark_results.xlsx - test results (put together in excel file from /log_results/ folder) and costs calculated.**

## Test Methodology

1. We ran the same workload under three different setups for each service:
  - 6 workers, no auto-scaling, with each worker having 4 vCPU and 16 GB memory. (this is Standard Glue workers size. In EMR we set up worker size through job run configuration)
  - 2 workers, no auto-scaling, with the same worker specifications
  - Auto-scaling. We let the service decide how to auto-scale, observing how it works using default settings.
2. We conducted three runs for each setup, measuring:
  - job execution time (also we pay attention to "warm-up" time needed to provision running infrastructure)
  - resource consumption relevant to billing (DPU-hours for Glue and vCPU-hours and memoryGb-hours for EMR Serverless).

## How to reproduce the tests on your own

1. Deploy all resources:

```bash
aws cloudformation deploy --stack-name cf-glue-vs-emr-serverless --template-file cloudformation/010_resources.yaml --capabilities CAPABILITY_NAMED_IAM
```

2. Copy NYC rides dataset into your bucket (it works for us-east-1 region)
```bash
export s3_bucket_name="s3-glue-vs-emr-serverless-$(aws sts get-caller-identity --query 'Account' --output text)"
aws s3 cp s3://nyc-tlc/opendata_repo/opendata_webconvert/yellow/ s3://${s3_bucket_name}/nyc_rides_2022_csv/ --recursive
```

3. Execute tests

Example:
```bash
python source/runner.py --test-file tests/glue_convert_to_parquet_fullyear.json
```
This runs testing scenario using Glue Jobs in 3 different setups: 6 workers (no scaling), 2 workers (no scaling), auto-scaling
and puts results in respective logs

For running tests using EMR Serverless, use another json file:
```bash
python source/runner.py --test-file tests/emrs_convert_to_parquet_fullyear.json
```

## Test results

For detailed test results, please refer to the article in this repository:
[Choosing between AWS Glue and EMR Serverless for your Big Data Workloads](../README.md)