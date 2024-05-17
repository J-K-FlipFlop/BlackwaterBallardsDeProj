# Project Blackwater - Totesys data extraction and formatting

Team Blackwater has been tasked with reading data from the Totesys remote Database into an AWS S3 Bucket via Lambda python code. This data should then be formatted into another S3 bucket by another Lambda function, and then output to an external Warehouse Database by a final Lambda function. All of the Lambda functions should be properly logged via Cloudwatch. Ultimately, the Database should be entering into Quicksight to show the project progress.

![alt text](image.png)

## Installation / Running the Code
The project provides a Makefile which can be run via the Make command. This will download into the local v-env all libraries and packages specified in the requirements.in file. This includes PEP8 compliance such as Black, and security packages such as Bandit and Safety. The v-env needs to be activated manually, as does exporting PYTHONPATH to ensure all the Python files can access the correct functions.

The project also needs a .env file, which is not included in any Git versions. This .env file ensures that a user has the correct permissions to access and run any of the AWS material. The .env should include an aws_access_key_id and aws_secret_access_key variable, which should link to the appropriate IAM user which has the specific permissions.

The IAM user should also be granted ability to view the relevant secrets on the AWS Secret Manager. This is because the credentials to connect to the totesys database (relevant code stored in src/credentials_manager.py), is stored remotely in AWS Secret Manager for security purposes.

The project was agreed to run on Python version 3.11.1, which is stated in the .python-version file.

## SRC
The src file contains all the Python code that is needed for the AWS Lambdas. The Lambdas are stored separately in different subfolders within the src file.

### Extract Lambda

#### connection
Accesses the username, password etc from the secrets manager required to form a connection to the totesys database. The only function, connect_to_db returns a pg8000 native connection to the totesys database.

#### credentials_manager
Using the relevant IAM user access key and secret access key stored in the .env file (see Installation guide), uses boto3 to access aws secrets manager and returns a dictionary containing the totesys connection information. This process is done in the get_secret function. Throws an error if cannot get a connection to the secrets manager.

#### example_update_code
The function update_data_in_bucket takes a table name as an arguement. Reads a previous runtime value from the S3 code bucket. If this value is not present, takes a default value of year 1999. The function then compares the previous runtime value to the last_updated time, and if the last_updated is later, then saves the whole sales entry to a list. This list of new info is then written to a new file in the bucket via the write_csv_to_s3 function, with a unique id in the title related to the exact time the function was run.

#### handler
Contains 3 (!!!) functions; connect_to_db_table, create_csv_data and (!!!). The connect_to_db_table takes a single table name as input, uses the connect_to_db function to connect to the database, and then runs SQL queries to select all data from the relevant table. This is stored with the column names in dictionary format. This is then converted into a Pandas dataframe, and then into a csv file.

#### upload
The upload file collects all of the relevant functions written in the previous files into one, which is then used as the source of the python code which is uploaded to Lambda. This file is needed so that we do not need to upload multiple python files for a lambda to work, nor need to use Lambda Layers.

## Terraform
The Terraform file contains all the relevant Terraform files to deploy the Lambdas, S3 buckets, Cloudwatch and IAM users required for the project.

### terraform_cloudwatch
(???)

### terraform_events
Creates the scheduler event which will periodically trigger the extract Lambda function to run every 5 minutes. Also connects the scheduler to the relevant rules to allow the scheduler to trigger the lambda function.

### terraform_iam
Creates the extract Lambda role, allowing the Lambda to put objects into the Ingestion Zone S3 bucket. Also creates and attaches the relevant policies and policy documents.

Additionally, creates and attachs 

## Test

