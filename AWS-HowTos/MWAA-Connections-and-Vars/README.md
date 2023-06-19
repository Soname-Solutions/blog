
# How to setup and use connections and variables in AWS managed Apache Airflow

Amazon Managed Workflows for Apache Airflow (MWAA) provides a very nice and easy way to manage Airflow Cluster.
Beyond the straightforward setup and maintenance of clusters, MWAA provides seamless integration with various AWS services.

One particularly useful integration is with AWS Secrets Manager, which allows you to store Airflow Connections and Variables securely. To name a few advantages of this approach:
- Centralized storage of connection details in a secure location
- The ability to share connections across multiple Airflow clusters and other services while keeping them maintained in one place
- Separate security or operations teams can easily maintain and update connection details, such as host changes or password rotations, without needing access to the Airflow cluster.

In this guide, we'll walk you through the step-by-step process of setting up and utilizing Secrets Manager-based connections and variables. Additionally, we've included CloudFormation templates and sample DAGs code, so you can easily integrate this solution into your project.

## Secrets manager-based Connections: How it works

A diagram below represents on a high-level how connections are referenced in Airflow DAGs and how connection details are retrieved from AWS Secrets manager.  

![High-level description](images/high-level-conn.png)

Properly setting up your Airflow cluster and deploying the necessary AWS resources is essential for this process. In this guide, we'll demonstrate the steps involved by providing examples and the corresponding source code.


## Settings in your Airflow cluster to work with Secrets Manager and connect to DB

In this section, we'll guide you through the process of creating a new Airflow cluster or modifying an existing one. In addition to the usual requirements, there are a few extra steps specific to our use case:
- Ensure that the **IAM Role** associated with your Airflow cluster has the necessary permissions to access secrets.
- Install the required Python libraries for interacting with the database by including them in your **requirements.txt** file.

Steps to create and properly set up Airflow cluster:
### 1. Ensure your VPC is set up correctly to host Airflow cluster
Before setting up the Airflow cluster, make sure your Virtual Private Cloud (VPC) meets the necessary networking requirements. You can refer to the AWS documentation for networking requirements [here](https://docs.aws.amazon.com/mwaa/latest/userguide/networking-about.html).

Alternatively, you can create a new VPC using the provided CloudFormation template from the Git source code of this article. Execute the following commands:
```bash
  export project_name="mwaa-secrets-demo"
  export stack_name="cfrm-${project_name}-010-vpc"

  aws cloudformation deploy \
    --template-file cloudformation/010_vpc.yaml \
    --stack-name $stack_name \
    --no-fail-on-empty-changeset \
    --parameter-overrides ProjectName=$project_name
```
*Note: we use `project_name` value as a part of the name of each resource in this demo.*

### 2. Create S3 bucket for Airflow Cluster and upload sample DAGs and requirements.txt files there

2.1 S3 creation using a CloudFormation Stack:
```bash
  export project_name="mwaa-secrets-demo"
  export stack_name="cfrm-${project_name}-015-s3"

  aws cloudformation deploy \
    --template-file cloudformation/015_s3.yaml \
    --stack-name $stack_name \
    --no-fail-on-empty-changeset \
    --parameter-overrides ProjectName=$project_name
```    

2.2 Deploy the DAGs and requirements.txt file

*Note: script determines s3 bucket name dynamically. If you changed the bucket name in previous step - please make changes to the following script accordingly*

```bash
  export project_name="mwaa-secrets-demo"
  account_id=$(aws sts get-caller-identity --query "Account" --output text | tr -d '\r')
  s3_bucket_name="s3-${project_name}-${account_id}"  

  aws s3 sync airflow/ s3://${s3_bucket_name}/ --delete
```   

### 3. Create Managed Airflow cluster and its IAM Role

*Notes: Airflow cluster requires security_group_id and private subnets ids as parameters. If you've created VPC using a template in step 1 - first commands in the script will automatically fetch the values. Otherwise, replace the parameters in the cloudformation deploy command with your own values.*

```bash
  export project_name="mwaa-secrets-demo"
  export stack_name="cfrm-${project_name}-020-mwaa"

    ################################################################################
    # get outputs (from CloudFormation VPC Stack)
    output=$(aws cloudformation list-exports)

    # Extract the values for the specific items using jq
    private_subnets_csv=$(echo "$output" | jq -r '.Exports[] | select(.Name == "vpc-mwaa-secrets-demo-private-subnets-csv") | .Value')
    security_group_id=$(echo "$output" | jq -r '.Exports[] | select(.Name == "vpc-mwaa-secrets-demo-security-group-id") | .Value')
    ################################################################################

  aws cloudformation deploy \
    --template-file cloudformation/020_mwaa.yaml \
    --stack-name $stack_name \
    --no-fail-on-empty-changeset \
    --parameter-overrides ProjectName=$project_name MWAAS3Bucket=$s3_bucket_name MWAASecurityGroupIds=$security_group_id MWAASubnetIds=$private_subnets_csv \
    --capabilities CAPABILITY_NAMED_IAM
```   

This cloudformation stack includes the following components:
- IAM policy that allows MWAA environment to read the values of stored secrets (Requirement #1 mentioned above).
![IAM Policy to access Secrets Manager values](images/iam_policy_secrets.png)

- MWAA Environment definition with the following important settings:
![IAM Policy to access Secrets Manager values](images/mwaa_definition.png)
    1. A reference to file requirements.txt, which contains list of database interaction libraries (Requirement #2 mentioned above)
    2. Declaration of Secrets Manager as the backend used for storing and retrieving connection and variable values.
    3. Definition of local OS variables, which will be discussed in the next section regarding working with variables.

Here's a sample content of **requirements.txt** file (assuming we're using MySQL and PostreSQL databases):
```
apache-airflow[mysql]
apache-airflow[postgres]
```

### 4. Create sample secrets
Let's create some sample secrets that we can use:

*Note: if you have a MySQL database to experiment with, please change the sample parameter values to your actual ones*

```bash
  export project_name="mwaa-secrets-demo"
  export stack_name="cfrm-${project_name}-025-secrets-mysql-and-var"

  aws cloudformation deploy \
    --template-file cloudformation/025_secrets_mysql_and_var.yaml \
    --stack-name $stack_name \
    --no-fail-on-empty-changeset \
    --parameter-overrides ProjectName=$project_name DBHost=sample_host DBLogin=sample_user DBPassword=sample_password DBDatabase=sample_dbname
```

Cloudformation stack creates three resources:
1. A secret containing the connection detail for the MySQL database in URI format
2. A secret in JSON format
3. A secret containing value for a variable - it will be used in "Variables" section.

#### **Secret String Format**

Two secrets are created in URI and JSON formats. These formats can be used interchangeably and define equivalent connections.
We create both formats for demonstration purposes.

URI Format:
```
mysql://login:password@DBHost:3306/database
```

JSON Format:
```json
  {
      "conn_type": "mysql",
      "login"    : "login",
      "password" : "password",
      "host"     : "DBHost",
      "database" : "database",
      "port"     : 3306
  }
```

When choosing which format to use, consider the following:
- JSON format for storing credentials available starting from Airflow version 2.3.0
- JSON format tends to be more readable in our opinion. Also, it might be more friendly for other secret value consumers (other scripts or services which might need to retrieve DB credentials)

You can find more information about secrets formats in the [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html).

*Note: If you prefer using PostgreSQL database - there's an alternative template available: cloudformation/030_secrets_postgresql.yaml. The differences are **conn_type** value and default DB port*

#### **Secrets Naming**

That's how secret names are defined:  
![Secret Names](images/secret_name.png)

Secret names start with a prefix that was defined in Airflow Environment creation template:  
![Secrets Prefix](images/secret_prefix_mwaa.png)

As a result in DAGs these connections can be refered by name, respectively, as:
- **aurora_mysql_uri**
- **aurora_mysql_json**

## Using connections - sample DAGs

Now let's log into our MWAA environment and test connections in our DAGs.  
The sample DAG we have for the test is "demo_connection_mysql". If you prefer to use PostgreSQL as your database, you can use the "demo_connection_postgresql" DAG instead. The differences between the two are minimal.

*Note: The full source code of DAGs can be found in the Git repository under `airflow\dags`.*

![dag list](images/demo_mysql_dag.png)

The DAG contains three operators:

![dag operators](images/demo_dag_operators.png)

1. **mysql_uri_connection** - which demonstrates using **MySQLOperator** to run "Create Table" command using connection defined in the URI format

```python
    mysql_uri_operator = MySqlOperator(
        task_id='mysql_uri_connection',
        mysql_conn_id = 'aurora_mysql_uri', # refers to secret named <connection_prefix>/aurora_mysql_uri
        sql=CREATE_SQL
    )
```

2. **execute_sql_json_connection** - uses **MySQLHook** inside a PythonOperator to execute simple select using connection defined in JSON format. As you may recall, both the URI and JSON formats are interchangeable.

```python
    conn_id = "aurora_mysql_json" # refers to secret named <connection_prefix>/aurora_mysql_json
    sql = SELECT_SQL

    result = []
    mysql_hook = MySqlHook(mysql_conn_id=conn_id)
    ...
```

3. **get_connection** - this operator retrieves and outputs both the URI and JSON connections to demonstrate that they result in the same connection properties.
![get_connection_output](images/get_connection_output.png)

## Using Variable values (Secrets / OS local variable)

Now let's explore several methods for defining variables in Airflow.

![High-level description](images/high-level-variables.png)

The first method is to define variables using AWS Secrets Manager, which is very similar to working with connections, as we described above.

Another method is to define OS environment variables, which is more suitable for values that rarely change. One real-life example is defining the environment name (e.g., dev/qa/prod).

If you have followed the cluster setup steps from the previous section, all the variables are already prepared for use. Let's proceed and test them.

### Retrieving variable value from Secrets Manager

In the MWAA cluster settings defined in **cloudformation/020_mwaa.yaml**, we specified the **variables_prefix** for the Secrets Manager backend:
```yaml
      AirflowConfigurationOptions:
        secrets.backend: airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
        secrets.backend_kwargs: !Sub '{"connections_prefix" : "${ProjectName}/connections", "variables_prefix" : "${ProjectName}/variables"}'
```

We have also created a test variable in **cloudformation/025_secrets_mysql_and_var.yaml**:
```yaml
  SecretMWAATestVariable:
    Type: AWS::SecretsManager::Secret
    Properties:
      Name: !Sub '${ProjectName}/variables/test_variable'
      SecretString: This is a Value of test variable (stored in secrets manager)
```

Now let's retrieve the value using the **demo_variable_secrets** DAG:

![demo_variable_secrets_dag](images/demo_variable_secrets_dag.png)

```python
  def print_vars_from_secret():
      var_names = ['test_variable'] # searches for a secret named "${ProjectName}/variables/test_variable"
      for item in var_names:
          value = Variable.get(item)
          print(f"---> {item} : {value}")
```

The output of the task will look like this:
```
[2023-06-07, 20:38:34 UTC] {{logging_mixin.py:137}} INFO - ---> test_variable : This is a Value of test variable (stored in secrets manager)
```

It's as simple as that!

### Retrieving environment variable value

During the creation of the cluster in **cloudformation/020_mwaa.yaml**, we defined two environment variables:
```yaml
      AirflowConfigurationOptions:
        ...
        os_var.variable1: value1
        env.variable2: 2023
```

Now let's access these variables running **demo_variable_env** DAG:
```python
  def print_defined_vars():
      var_names = ['AIRFLOW__OS_VAR__VARIABLE1','AIRFLOW__ENV__VARIABLE2']

      for item in var_names:
          value = os.environ[item]
          print(f"---> {item} : {value}")
```
**Please note the name change:**
- CloudFormation template:
  - os_var.variable1
  - env.variable2
- Accessing from DAG ("AIRFLOW_" prefix, all uppercase, "." is replaced by "__")
  - AIRFLOW__OS_VAR__VARIABLE1
  - AIRFLOW__ENV__VARIABLE2

The output of the task will be:
```
[2023-06-07, 20:51:06 UTC] {{logging_mixin.py:137}} INFO - ---> AIRFLOW__OS_VAR__VARIABLE1 : value1
[2023-06-07, 20:51:06 UTC] {{logging_mixin.py:137}} INFO - ---> AIRFLOW__ENV__VARIABLE2 : 2023
```

### Methods comparison

Variables stored in AWS Secrets Manager provide more flexibility, as you can change their values by simply modifying the corresponding secret. On the other hand, changing environment variables requires updating the cluster settings, which is a time-consuming operation that takes around 20-30 minutes.

However, it's important to note that using secret-based variables incurs charges for each secret stored, while environment variables do not have any additional cost.


## Considerations when using Connections and Variables in Secrets Manager

It's important to consider the cost implications when utilizing Connections and Variables in Secrets Manager. Remember that you will incur charges for both storing and accessing each individual secret.

As of now, the pricing for Secrets Manager is $0.40 per secret per month, and there is an additional cost of $0.05 per 10,000 API calls. Please note that pricing is subject to change, so it's advisable to refer to the [AWS Documentation](https://aws.amazon.com/secrets-manager/pricing/?nc1=h_ls) for the most up-to-date pricing information.

## Clean Up

By following these steps, you will clean up all the resources created for the demonstration.

### 1. Delete secrets

```bash
export project_name="mwaa-secrets-demo"
aws secretsmanager delete-secret --secret-id ${project_name}/connections/aurora_mysql_uri --force-delete-without-recovery 
aws secretsmanager delete-secret --secret-id ${project_name}/connections/aurora_mysql_json --force-delete-without-recovery 
aws secretsmanager delete-secret --secret-id ${project_name}/connections/aurora_postgresql_uri --force-delete-without-recovery 
aws secretsmanager delete-secret --secret-id ${project_name}/connections/aurora_postgresql_json --force-delete-without-recovery 
aws secretsmanager delete-secret --secret-id ${project_name}/variables/test_variable --force-delete-without-recovery 

export stack_name="cfrm-${project_name}-025-secrets-mysql-and-var"

  aws cloudformation delete-stack --stack-name ${stack_name}
  aws cloudformation wait stack-delete-complete --stack-name ${stack_name}

export stack_name="cfrm-${project_name}-030-secrets-postgresql"

  aws cloudformation delete-stack --stack-name ${stack_name}
  aws cloudformation wait stack-delete-complete --stack-name ${stack_name}  
```    

*Note: Please note that we have explicitly deleted the secrets using the "force-delete-without-recovery" option, rather than relying on CloudFormation. This approach allows us to delete and recreate the sample stacks multiple times.*

*If we don't use this option, AWS performs a "soft delete" of the secret, which means it is not immediately deleted and can still be recovered. However, if you attempt to create a stack with the same name within the next 30 days, it will result in an error due to the secret being in a deleted state.*

*By using the "force-delete-without-recovery" option, we ensure that the secrets are immediately and permanently deleted, allowing us to create new stacks without any issues.*


### 2. Delete Airflow Cluster

```bash
export project_name="mwaa-secrets-demo"
export stack_name="cfrm-${project_name}-020-mwaa"

  aws cloudformation delete-stack --stack-name ${stack_name}
  aws cloudformation wait stack-delete-complete --stack-name ${stack_name}
```    

### 3. Delete S3 bucket

```bash
export project_name="mwaa-secrets-demo"
export stack_name="cfrm-${project_name}-015-s3"

  # empty bucket
  account_id=$(aws sts get-caller-identity --query "Account" --output text | tr -d '\r')
  s3_bucket_name="s3-${project_name}-${account_id}"  
  sh scripts/util_empty_versioned_s3_bucket.sh ${s3_bucket_name}

  aws cloudformation delete-stack --stack-name ${stack_name}
  aws cloudformation wait stack-delete-complete --stack-name ${stack_name}
``` 

*Note: we include a shell script that empties S3 versioned bucket. This step is necessary as a prerequisite for deleting the bucket using the CloudFormation stack.*

### 4. Delete VPC Stack

```bash
  export project_name="mwaa-secrets-demo"
  export stack_name="cfrm-${project_name}-010-vpc"

  aws cloudformation delete-stack --stack-name $stack_name
  aws cloudformation wait stack-delete-complete --stack-name $stack_name
```  