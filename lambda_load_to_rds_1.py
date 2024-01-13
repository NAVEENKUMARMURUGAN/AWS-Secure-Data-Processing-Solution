import json
import psycopg2
import pyarrow.parquet as pq
from io import BytesIO
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Extract information from the S3 event
        # s3_bucket = event['Records'][0]['s3']['bucket']['name']
        # s3_key = event['Records'][0]['s3']['object']['key']
        
        s3_bucket = event['s3_bucket']
        s3_key = event['s3_key']
        load = event['load']
        
        print(s3_bucket)
        print(s3_key)

        # Read data from S3
        parquet_data = read_data_from_s3(s3_bucket, s3_key)
        
        print("parquet reading completed and proceeding to postgres ")
        table_name = determine_target_table(s3_key)

        # Load data into PostgreSQL
        load_data_to_postgresql(parquet_data, table_name)

        return {
            'statusCode': 200,
            'body': json.dumps('Lambda function executed successfully!'),
            's3_bucket': f'{s3_bucket}',
            's3_key': f'{s3_key}',
            'load': f'{load}'
        }
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}'),
            's3_bucket': f'{s3_bucket}',
            's3_key': f'{s3_key}',
            'load': f'{load}'
        }

def read_data_from_s3(s3_bucket, s3_key):
    try:
        s3_object = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        data = s3_object['Body'].read()
        
        print('data', data)
        # Step 1: Read Parquet data
        parquet_data = pq.read_table(BytesIO(data))
        
        print('parquet_data', parquet_data)

        return parquet_data
    except Exception as e:
        print(f"Error reading data from S3: {e}")
        raise e

def load_data_to_postgresql(parquet_data, table_name):
    try:
        # Your PostgreSQL connection parameters
        db_params = {
            'dbname': 'myretaildatabase',
            'user': 'postgres',
            'password': 'postgres',
            'host': 'myretailinstance.ctn92obk1nsu.ap-southeast-2.rds.amazonaws.com',
            'port': '5432'
        }

        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)

        # Create a cursor
        cursor = conn.cursor()

        
        # Assuming parquet_data is your PyArrow Table
        data_values = []
        
        # Iterate over columns
        for row_index in range(parquet_data.num_rows):
            # Extract values from each column for the current row
            row_data = [column[row_index].as_py() for column in parquet_data.itercolumns()]
    
            # Append the tuple to the data_values list
            data_values.append(tuple(row_data))

        print(data_values)
        
        # Insert data into PostgreSQL
        if table_name == 'customer_profiles':
            insert_query = f"INSERT INTO {table_name} (customerid, name, email, address, phone) VALUES (%s, %s, %s, %s, %s);"
        elif table_name == 'purchasing_patterns':
            insert_query = f"INSERT INTO {table_name} (transactionid, customerid, productid, quantity, totalamount, timestamp) VALUES (%s, %s, %s, %s, %s, %s);"
        elif table_name == 'promotional_engagement':
            insert_query = f"INSERT INTO {table_name} (customerid, promotionid, promotionname, engagementdate, engagementtype) VALUES (%s, %s, %s, %s, %s);"
            
        cursor.executemany(insert_query, (data_values))  # Replace 'column_name' with your actual column name

        # Commit and close the connection
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
        raise e

def determine_target_table(filename):
    # Implement your logic to determine the target table based on the Parquet file name
    if "customer_profiles" in filename:
        return "customer_profiles"
    elif "purchasing_patterns" in filename:
        return "purchasing_patterns"
    elif "promotional_engagement" in filename:
        return "promotional_engagement"
    else:
        return "default_table"
