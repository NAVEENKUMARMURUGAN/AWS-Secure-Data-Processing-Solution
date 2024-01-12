import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
from botocore.exceptions import NoCredentialsError
from datetime import datetime
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from os import urandom
import os
import json

# Sample data
customer_profiles_data = {
    'CustomerID': [1, 2, 3],
    'Name': ['John Doe', 'Jane Smith', 'Mike Johnson'],
    'Email': ['john.doe@email.com', 'jane.smith@email.com', 'mike.johnson@email.com'],
    'Address': ['123 Main St, City', '456 Oak St, Town', '789 Pine St, Village'],
    'Phone': ['555-123-4567', '555-987-6543', '555-555-5555']
}

purchasing_patterns_data = {
    'TransactionID': [101, 102, 103],
    'CustomerID': [1, 2, 1],
    'ProductID': [1001, 1002, 1003],
    'Quantity': [2, 1, 3],
    'TotalAmount': [150.00, 75.00, 200.00],
    'Timestamp': ['2023-01-15 08:45:00', '2023-01-15 09:30:00', '2023-01-16 10:15:00']
}

promotional_engagement_data = {
    'CustomerID': [1, 2, 3],
    'PromotionID': [501, 502, 503],
    'PromotionName': ['Winter Sale', 'Clearance Event', 'Flash Sale'],
    'EngagementDate': ['2023-01-10 12:00:00', '2023-01-11 14:30:00', '2023-01-12 16:45:00'],
    'EngagementType': ['Click', 'View', 'Purchase']
}

# Convert data to dataframes
customer_profiles_df = pd.DataFrame(customer_profiles_data)
purchasing_patterns_df = pd.DataFrame(purchasing_patterns_data)
promotional_engagement_df = pd.DataFrame(promotional_engagement_data)

# Save dataframes to Parquet files
customer_profiles_buffer = BytesIO()
purchasing_patterns_buffer = BytesIO()
promotional_engagement_buffer = BytesIO()

pq.write_table(pa.Table.from_pandas(customer_profiles_df), customer_profiles_buffer)
pq.write_table(pa.Table.from_pandas(purchasing_patterns_df), purchasing_patterns_buffer)
pq.write_table(pa.Table.from_pandas(promotional_engagement_df), promotional_engagement_buffer)

# Assume Role
role_arn = 'arn:aws:iam::253722483539:role/abc-s3-kms-ro-role'
role_session_name = 'AssumedRoleSession'
source_access_key = 'AKIATWEYJ3NJ2K5LUVHK'
source_secret_key = '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'

sts_client = boto3.client('sts', aws_access_key_id=source_access_key, aws_secret_access_key=source_secret_key)

# Assume role using IAM user's credentials
assumed_role_object = sts_client.assume_role(
    RoleArn=role_arn,
    RoleSessionName=role_session_name
)

# Extract temporary credentials
credentials = assumed_role_object['Credentials']

# Use the assumed role credentials to create a session
session = boto3.Session(
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken']
)

# Create S3 and KMS clients using the assumed role session
s3 = session.client('s3', region_name='ap-southeast-2')
kms = session.client('kms', region_name='ap-southeast-2')

kms_key_id = 'arn:aws:kms:ap-southeast-2:253722483539:key/e44d1d6f-b242-4f5a-a02f-285957fe2f17'

def generate_data_key(kms_client, key_id):
    # Generate a data key
    response = kms_client.generate_data_key(
        KeyId=key_id,
        KeySpec='AES_256'
    )
    return response['CiphertextBlob'], response['Plaintext']

encrypted_dek, plaintext_dek  = generate_data_key(kms, kms_key_id)

iv = urandom(16)

cipher = Cipher(algorithms.AES(plaintext_dek), modes.CFB8(iv), backend=default_backend())
customer_profiles_encryptor = cipher.encryptor()
customer_profiles_encrypted_buffer = customer_profiles_encryptor.update(customer_profiles_buffer.getvalue()) + customer_profiles_encryptor.finalize()

purchasing_patterns_encryptor = cipher.encryptor()
purchasing_patterns_encrypted_buffer = purchasing_patterns_encryptor.update(purchasing_patterns_buffer.getvalue()) + purchasing_patterns_encryptor.finalize()

promotional_engagement_encryptor = cipher.encryptor()
promotional_engagement_encrypted_buffer = promotional_engagement_encryptor.update(promotional_engagement_buffer.getvalue()) + promotional_engagement_encryptor.finalize()

# Upload encrypted Parquet files to S3
s3_bucket_name = 'abc-dev01-incoming-files'
s3_prefix = 'data'

s3.upload_fileobj(
    BytesIO(customer_profiles_encrypted_buffer),
    s3_bucket_name,
    f'{s3_prefix}/customer_profiles.parquet.encrypted'
)

print(f"encrypted data file {s3_prefix}/customer_profiles.parquet.encrypted uploaded to S3.")

s3.upload_fileobj(
    BytesIO(purchasing_patterns_encrypted_buffer),
    s3_bucket_name,
    f'{s3_prefix}/purchasing_patterns.parquet.encrypted'
)

print(f"encrypted data file {s3_prefix}/purchasing_patterns.parquet.encrypted uploaded to S3.")

s3.upload_fileobj(
    BytesIO(promotional_engagement_encrypted_buffer),
    s3_bucket_name,
    f'{s3_prefix}/promotional_engagement.parquet.encrypted'
)

print(f"encrypted data file {s3_prefix}/promotional_engagement.parquet.encrypted uploaded to S3.")

# Create control files
control_files = []

# Customer Profiles
customer_profiles_control_content = {
    'filename': 'customer_profiles.parquet.encrypted',
    'business_date': datetime.now().strftime('%Y-%m-%d'),
    'count': len(customer_profiles_df),
    'iv': iv.hex(),
    'encrypted_data_key': encrypted_dek.hex()
}
customer_profiles_control_path = 'customer_profiles.json'
control_files.append((customer_profiles_control_content, customer_profiles_control_path))

# Purchasing Patterns
purchasing_patterns_control_content = {
    'filename': 'purchasing_patterns.parquet.encrypted',
    'business_date': datetime.now().strftime('%Y-%m-%d'),
    'count': len(purchasing_patterns_df),
    'iv': iv.hex(),
    'encrypted_data_key': encrypted_dek.hex()
}
purchasing_patterns_control_path = 'purchasing_patterns.json'
control_files.append((purchasing_patterns_control_content, purchasing_patterns_control_path))

# Promotional Engagement
promotional_engagement_control_content = {
    'filename': 'promotional_engagement.parquet.encrypted',
    'business_date': datetime.now().strftime('%Y-%m-%d'),
    'count': len(promotional_engagement_df),
    'iv': iv.hex(),
    'encrypted_data_key': encrypted_dek.hex()
}
promotional_engagement_control_path = 'promotional_engagement.json'
control_files.append((promotional_engagement_control_content, promotional_engagement_control_path))

# Upload control files to S3
for content, path in control_files:
    with open(path, 'w') as control_file:
        json.dump(content, control_file)

    s3.upload_file(
        path,
        s3_bucket_name,
        f'{s3_prefix}/{path}'
    )

    os.remove(path)

    print(f"Control file '{path}' uploaded to S3.")

print("Control files created and uploaded to S3.")
