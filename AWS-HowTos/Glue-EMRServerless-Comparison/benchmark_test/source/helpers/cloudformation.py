import boto3


def get_stack_outputs(stack_name):
    # Initialize a boto3 client for CloudFormation
    cf_client = boto3.client("cloudformation")
    outp = {}

    try:
        # Retrieve stack details
        response = cf_client.describe_stacks(StackName=stack_name)

        # Check if the stack has outputs
        if "Outputs" in response["Stacks"][0]:
            for output in response["Stacks"][0]["Outputs"]:
                key = output["OutputKey"]
                value = output["OutputValue"]
                outp[key] = value

            return outp
        else:
            print(f"No outputs found for stack '{stack_name}'.")

    except cf_client.exceptions.ClientError as e:
        print(f"An error occurred: {e.response['Error']['Message']}")
