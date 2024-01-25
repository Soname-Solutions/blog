import boto3
import json
from datetime import datetime
import time
from urllib.parse import urlparse

from helpers.s3_helper import upload_file_to_s3

States_Success = ["SUCCEEDED"]
States_Failure = ["FAILED", "ERROR", "TIMEOUT", "STOPPED"]
Final_States = States_Success + States_Failure


def start_job_run(glue_client, job_name, arguments, logger):
    response = glue_client.start_job_run(JobName=job_name, Arguments=arguments)
    job_run_id = response["JobRunId"]
    logger.info(f"Job Run started. Job Run ID: {job_run_id}")
    return job_run_id, response


def watch_job_run(glue_client, job_name, job_run_id, logger):
    state_durations = {}  # Dictionary to store durations for each state
    last_state_change = datetime.now()  # Initialize the time for state change
    current_state = None

    response = ""
    # Waiting for Job Run to Complete (reach final state)
    while current_state not in Final_States:
        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        new_state = response["JobRun"]["JobRunState"]

        if new_state != current_state:
            current_time = datetime.now()
            if current_state is not None:
                duration = (current_time - last_state_change).total_seconds()
                state_durations[current_state] = duration
                logger.info(f"State {current_state}: {duration} seconds")

            last_state_change = current_time
            current_state = new_state

        logger.info(new_state)
        time.sleep(2)

    # Add the duration for the final state
    duration = (datetime.now() - last_state_change).total_seconds()
    state_durations[current_state] = duration

    if is_run_autoscale(glue_client, job_name, job_run_id):        
        i = 0
        while "DPUSeconds" not in response["JobRun"]:
            logger.info(f"Waiting for DPUSeconds to be filled in")
            response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
            time.sleep(2)
            i += 1
            if i > 100:
                raise Exception("Can't retrieve DPUSeconds stats")            

    execution_time = response["JobRun"]["ExecutionTime"]
    allocated_capacity = response["JobRun"].get("AllocatedCapacity", None)
    dpu_seconds = response["JobRun"].get("DPUSeconds", None)

    if dpu_seconds is not None:
        dpu_hours = float(dpu_seconds) / 60 / 60
    else:
        dpu_hours = float(execution_time) / 60 / 60 * float(allocated_capacity)

    dpu_hours = round(dpu_hours, 3)

    output = {
        "state_durations": state_durations,
        "execution_time_sec": execution_time,
        "allocated_capacity": allocated_capacity,
        "dpu_seconds": dpu_seconds,
        "dpu_hours": dpu_hours,
        "response": response,
    }

    return output


def is_run_autoscale(glue_client, job_name, job_run_id):
    argument_name = "--enable-auto-scaling"

    response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
    arguments: dict = response["JobRun"].get("Arguments", {})
    is_autoscale_run = (
        argument_name in arguments.keys() and arguments[argument_name] == "true"
    )

    response = glue_client.get_job(JobName=job_name)
    default_arguments: dict = response["Job"]["DefaultArguments"]
    is_autoscale_job = (
        argument_name in default_arguments.keys()
        and default_arguments[argument_name] == "true"
    )

    return is_autoscale_job or is_autoscale_run


def get_job_script_location(glue_client, job_name):
    response = glue_client.get_job(JobName=job_name)
    script_location = response["Job"]["Command"]["ScriptLocation"]
    return script_location


def upload_glue_script_to_job_location(local_script_fullpath, s3_script_location):
    s3_path_parts = urlparse(s3_script_location, allow_fragments=False)
    s3_bucket_name, s3_prefix = s3_path_parts.netloc, s3_path_parts.path.lstrip("/")
    upload_file_to_s3(local_script_fullpath, s3_bucket_name, s3_prefix)


def run_test(glue_client, local_script_fullpath, job_name, arguments, logger):
    # update glue job script to the one required for the test
    s3_script_location = get_job_script_location(glue_client, job_name)
    upload_glue_script_to_job_location(local_script_fullpath, s3_script_location)

    #################################################################

    glue_client = boto3.client("glue")

    start = time.perf_counter()

    # Starting Job Run
    args = {
        "glue_client": glue_client,
        "job_name": job_name,
        "arguments": arguments,
        "logger": logger,
    }
    job_run_id, response = start_job_run(**args)

    logger.info(response)

    # Waiting for Job Run to Complete and collect stats
    args = {
        "glue_client": glue_client,
        "job_name": job_name,
        "job_run_id": job_run_id,
        "logger": logger,
    }
    output = watch_job_run(**args)

    end = time.perf_counter()

    formatted_output = json.dumps(output, indent=4, default=str)

    # Print out Run stats
    logger.info(f"Total Running Time: {end - start:0.4f} seconds")
    logger.info(f"Output:\n{formatted_output}")
