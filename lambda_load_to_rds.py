import boto3
import json
import pyarrow.parquet as pq
from io import BytesIO
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import logging
import jsonpickle
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
kms_client = boto3.client('kms', region_name='ap-southeast-2')  # Replace 'your-region' with the appropriate AWS region

def lambda_handler(event, context):
    try:
        logger.info('## ENVIRONMENT VARIABLES\r' + jsonpickle.encode(dict(**os.environ)))
        logger.info('## EVENT\r' + jsonpickle.encode(event))
        logger.info('## CONTEXT\r' + jsonpickle.encode(context))

        # Extract information from the S3 event
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_key = event['Records'][0]['s3']['object']['key']
        
        logger.info('## S3 Bucket\r' + s3_bucket)
        logger.info('## S3 Key\r' + s3_key)


        control_file_data = fetch_control_file_data(s3_bucket, s3_key)
        
                       
        decrypted_data = decrypt_data(control_file_data, s3_bucket)
        
        compare_counts(control_file_data, decrypted_data, s3_bucket)

        # Put the decrypted data back to S3
        decrypted_s3_key = f'decrypted/{control_file_data["filename"]}'
        put_decrypted_data_to_s3(s3_bucket, decrypted_s3_key, decrypted_data)

        return {
            'statusCode': 200,
            'body': json.dumps('Lambda function executed successfully!')
        }
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def fetch_control_file_data(s3_bucket, s3_key):
    try:
        control_file_content = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)['Body'].read()
        control_file_data = json.loads(control_file_content)
        logger.info("Control file data: %s", control_file_data)
        return control_file_data
    except Exception as e:
        logger.error(f"Error fetching S3 object: {e}")
        raise e

def decrypt_data(control_file_data, s3_bucket):
    try:
        encrypted_file_key = bytes.fromhex(control_file_data['encrypted_data_key'])
        iv = bytes.fromhex(control_file_data['iv'])

        decrypted_data_key = kms_client.decrypt(CiphertextBlob=encrypted_file_key)['Plaintext']

        encrypted_data_file = s3_client.get_object(Bucket=s3_bucket, Key=f'data/{control_file_data["filename"]}')
        encrypted_data = encrypted_data_file['Body'].read()

        cipher = Cipher(algorithms.AES(decrypted_data_key), modes.CFB8(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()

        return decrypted_data
    except Exception as e:
        logger.error(f"Error in decrypt_data: {e}")
        raise e

def put_decrypted_data_to_s3(s3_bucket, s3_key, decrypted_data):
    try:
        s3_client.put_object(Body=decrypted_data, Bucket=s3_bucket, Key=s3_key)
    except Exception as e:
        logger.error(f"Error putting decrypted data to S3: {e}")
        raise e
        
def compare_counts(control_file_data, decrypted_data, s3_bucket):
    try:
        # Read the control file count
        control_file_count = control_file_data.get('count')

        # Get the Parquet file count
        parquet_count = get_parquet_record_count(decrypted_data)

        # Compare counts
        if control_file_count == parquet_count:
            logger.info(f"Count matches :control_file_count {control_file_count} and  parquet_count {parquet_count}")
        else:
            logger.error(f"Count doesn't match :control_file_count {control_file_count} and  parquet_count {parquet_count}")
            
    except Exception as e:
        print(f"Error during count comparison: {e}")
        return False

def get_parquet_record_count(decrypted_data):
    try:
        # Assuming decrypted_data is a byte string containing Parquet data
        parquet_buffer = BytesIO(decrypted_data)
        parquet_table = pq.read_table(parquet_buffer)

        # Get the number of rows in the Parquet table
        parquet_count = len(parquet_table)

        return parquet_count
    except Exception as e:
        print(f"Error getting record count from Parquet data: {e}")
        return None
