# Project Blackwater - Totesys data extraction and formatting

Team Blackwater has been tasked with reading data from the Totesys remote Database into an AWS S3 Bucket via Lambda python code. This data should then be formatted into another S3 bucket by another Lambda function, and then output to an external Warehouse Database by a final Lambda function. All of the Lambda functions should be properly logged via Cloudwatch. Ultimately, the Database should be entering into Quicksight to show the project progress.

![alt text](image.png)

## Installation / Running the Code
The project provides a Makefile which can be run via the Make command. This will download into the local v-env all libraries and packages specified in the requirements.in file. This includes PEP8 compliance such as Black, and security packages such as Bandit and Safety. The v-env needs to be activated manually, as does exporting PYTHONPATH to ensure all the Python files can access the correct functions.

The project also needs a .env file, which is not included in any Git versions. This .env file ensures that a user has the correct permissions to access and run any of the AWS material. The .env should include an aws_access_key_id and aws_secret_access_key variable, which should link to the appropriate IAM user which has the specific permissions.

The IAM user should also be granted ability to view the relevant secrets on the AWS Secret Manager. This is because the credentials to connect to the totesys database (relevant code stored in src/extract_lambda/credentials_manager.py), is stored remotely in AWS Secret Manager for security purposes.

The project was agreed to run on Python version 3.11.1, which is stated in the .python-version file.

## SRC
The src file contains all the Python code that is needed for the AWS Lambdas. The Lambdas are stored separately in different subfolders within the src file.

### Extract Lambda

#### connection
Accesses the username, password etc from the secrets manager required to form a connection to the totesys database. The only function, connect_to_db returns a pg8000 native connection to the totesys database.

#### credentials_manager
Using the relevant IAM user access key and secret access key stored in the .env file (see Installation guide), uses boto3 to access aws secrets manager and returns a dictionary containing the totesys connection information. This process is done in the get_secret function. Throws an error if cannot get a connection to the secrets manager.

#### handler
Contains only 1 function, lambda_handler, which takes a boto3 session as an argument. Using a hard-coded list of table names from the Totesys Database (can be done programatically if need be), for each table in the database, uses the write_csv_to_s3 utility function to write the table data to the specified bucket. The table name is written in as part of the file path to the relevant storage location in the bucket. The function logs a success message if successful, or logs an error message if unsuccessful, and returns the message given. 

#### utils
The utils file contains all of the utility functions needed to run the code. It contains 4 functions; sql_security, convert_table_to_dict, write_csv_to_s3 and update_data_in_bucket.

The function sql_security takes a table name as an argument. It then connects to the Totesys database using the connect_to_db function, and selects all the table names from the database. As pg8000 attaches many other methods here, the table names are then filtered to give only the wanted values. If the entered table name is in the list of programatically generated table names, the function returns the table name. If not, the function returns a DatabaseError.

The function convert_table_to_dict takes a table name as argument. It first uses sql_security to check if this table name is secure. It the connects to the Totesys database using the connect_to_db function, and runs an sql query to select all data from the table specified as the argument. It zips and collects the data headers and returns the info as a list of dicts. The function throws a DatabaseError if that table name is not accessable in the database.

The function write_csv_to_s3 takes a session, data to be written, bucket name and key (file path to data) as arguments. It uses the wrangler module and to_csv inbuilt functions to write the given data to the specified s3 bucket. The data is converted to a pandas dataframe before entry, and the bucket name and key provide the file path to the stored data. Returns a success message dict on successful write, throws a ClientError and logs the error on a failure.

The function update_data_in_bucket takes a table name as an argument. Reads a previous runtime value from the S3 code bucket. If this value is not present, takes a default value of year 1999. The function then compares the previous runtime value to the last_updated time, and if the last_updated is later, then saves the whole sales entry to a list. This list of new info is then written to a new file in the bucket via the write_csv_to_s3 function, with a unique id in the title related to the time the function was run. Finally, the time the function is run is then written to the bucket, overwriting the previous runtime. This ensures that the next instance of the code will know the last run time and will then write only the relevant entries. Logs and returns any response given.

### Transform Lambda

#### Utils
Contains 3 functions; read_latest_changes, get_data_from_ingestion_bucket, and write_parquet_data_to_s3. The function write_parquet_data_to_s3 is imported to the handler.py file, read_latest_changes is imported to both handler.py and transform_funcs.py, and get_data_from_ingestion_bucket is imported just to transform_funcs.py.

The read_latest_changes function takes a boto3 client as an argument, and gets a list of the keys from the boto3 client s3 bucket that are most recently updated. It returns a dictionary with a success message, timestamp and retrieved list of files. If a ClientError is thrown during thr process, the function returns a dictionary with a failure message.

The get_data_from_ingestion_bucket function reads the csv data from the s3 ingestion zone and converts it into a pandas dataframe. The function takes 3 arguments; key, filename and a boto3 session. The boto3 session determines which s3 to read from, and the key and filename determine which file in the s3 to read. The function returns a dictionary containing a success message and the dataframe on a success, or returns a dictionary containing a failure message and error response on either a CLientError or NoFilesFound error.

The write_parquet_data_to_s3 functions takes a pandas dataframe, table_name, boto3 session and timestamp as arguments. The function writes the dataframe to the processed zone specified in the boto3 session. The table_name and timestamp determine the filepath of the file written to the processed zone. Returns a dictionary containing a success message on a successful write, or a dictionary with a failure message on either a ClientError or generally on a failed write.

#### transform_funcs
The transform functions use the read_latest_changes and get_data_from_ingestion_bucket utility functions to access and transform the data from each table in the Totesys database. Each function takes a client and session as arguments, and returns an output dictionary containing a success status message and a dataframe. There are unique functions for each table that exist in the final database schema, for example design, currency, staff, counterparty, etc. These functions are specific to each table in the final database as the structure of each table, the datatypes of each column, the form that the data is originally kept in the Totesys database, etc, are all different, so require specific code to format them correctly.

#### handler
The handler file contains 1 function, lambda_handler. All of the functions from transform_funcs.py are imported into handler, which runs each of them and stores them as variables. On checking these variables for a success status message, the function either writes this data to the processed zone using the write_parquet_data_to_s3 function, or prints and logs that no data was written. If any data has been written, the last_ran_at.csv at the root of the processed zone s3 bucket is updated, which will trigger the load lambda to run. Finally, the function prints and logs the number of tables updated.

### Load Lambda

#### credentials manager
Contains a single function, get_secret. Uses AWS IAM roles to access the AWS Secrets Manager and retrieve the credentials for the Warehouse database. Returns a dictionary of the secret values.

#### connection
Uses the get_secret function from credentials_manager.py to access the Secret Values for the Warehouse database, and forms a pg8000 connection to that database with the connect_to_db function. This function returns the Connection.

#### utils
Contains 5 functions; sql_security, get_latest_processed_file_list, get_data_from_processed_zone, get_insert_query and insert_data_into_data_warehouse. 


## Terraform
The Terraform file contains all the relevant Terraform files to deploy the Lambdas, S3 buckets, Cloudwatch and IAM users required for the project.

### terraform_alarms
Contains the cloudwatch logs, metrics and filters to handle any errors thrown by any of the Lambdas. When a single error is given by the code, will send an email update to the given email addresses, alerting them to the specific error.

### terraform_events
Creates the scheduler event which will periodically trigger the extract Lambda function to run every 5 minutes. Also connects the scheduler to the relevant rules to allow the scheduler to trigger the lambda function.

### terraform_iam
Creates the extract Lambda role, allowing the Lambda to put objects into the Ingestion Zone S3 bucket. Also creates and attaches the relevant policies and policy documents.

Additionally, creates and attachs the relevant Cloudwatch policies, such as creating log groups and log streams, between the extract lambda and Cloudwatch.

Finally, allows the lambda to access the secrets manager, which takes the secret values to allow the code to access the Totesys database in a secure way.

### terraform_lambda
Creates the extract_lambda function within AWS lambda. Establishes where the extract_lambda pulls it's code from, and the layers available to the lambda. Also establishes where the lambda function should be zipped to, and links the permissions for the extract_lambda to be invoked by eventbridge. Finally, creates an aws wrangler to manage the layers available to the lambda.

### terraform_main
Sets up some of the default parameters for terraform, such as the terraform state bucket, the source and version, the aws regions and logs in with the access key and secret access key provided in the locally-saved .env file.

### terraform_s3
Sets up the buckets which will store the data ingested and transformed by the lambdas. 

### terraform_variable
Gives access to the environment variables for terraform.

## Test
Contains the testing for the python code.

### test_lamb_handler
Using pytest fixtures, establishes a mock aws client to test the lambda code.

#### test_handler_returns_false_message
Takes an s3_client as an argument. Connects to the mocked boto3 client, enters in some incorrect values and checks for a error message detailing the incorrect bucket name.

#### test_handler_returns_true_message
Takes an s3_client as an argument. Creates a bucket locally on the mocked boto3 client, and then connects to the bucket with nonesense data. Asserts a successful connection message about writing to the bucket.

#### test_handler_writes_correct_number_of_files_to_bucket
Takes an s3_client as an argument. Creates a bucket and writes to it, and then lists the objects inside, asserting the contents of the bucket are the same as the values entered.

#### test_handler_writes_data_to_each_file
Takes an s3_client as an argument. Creates a bucket and writes to it, and then lists the contents of each file. Asserts the size of each file is below 100.

### test_utils
Using pytest fixtures, establishes a mock aws client to test the lambda code.

#### test_function_returns_a_list_of_dicts
Testing the convert_table_to_dict function. Reads the sales_order table from Totesys, and asserts that every value in the response list is of the type dictionary.

#### test_header_count_equal_to_result_count
Testing the convert_table_to_dict function. Asserts the first value in the list is the same length as a list containing all the column headers.

#### test_returns_error_message_if_table_name_not_found
Testing the convert_table_to_dict function. Asserts a DatabaseError is raised if passing the wrong name to the function.

#### test_sql_statement_not_vulnerable_to_injection
Testing the convert_table_to_dict function. Passes in a dangerous SQL phrase, involving dropping the table. Asserts that the function raises a DatabaseError on this attempt at malicious injection.

#### test_s3_takes_file
Testing the write_to_s3 function. Passes in correctly formatted info to a bucket that is just created, and asserts the message confirms the writing has gone through.

#### test_s3_uploads_correct_file_content
Testing the write_to_s3 function. Checks the content of data uploaded to a fake bucket via a get object command. Asserts the body of the reponse is the same as the data entered.

#### test_write_to_s3_fails_when_no_bucket
Testing the write_to_s3 function. Asserts an error value when trying to write to a bucket that does not exist.

#### test_write_to_s3_works_with_csv
Testing the write_to_s3 function. Asserts that the returned value of data stored to the bucket is correct when it was originally in csv format.

#### test_csv_file_is_written_to_bucket
Testing the write_csv_to_s3 function. Creates a mock boto3 session, and checks the result of the function is a successful message.

#### test_csv_uploads_correct_file_content
Testing the write_csv_to_s3 function. Asserts that the body of data entered to the bucket by the function is correct.

#### test_write_fails_when_bucket_not_found
Testing the write_csv_to_s3 function. Asserts the function throws an error message when the fake bucket has not been created.