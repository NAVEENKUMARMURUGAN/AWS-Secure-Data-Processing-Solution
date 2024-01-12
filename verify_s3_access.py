import boto3

def list_files_in_bucket(access_key, secret_key, role_arn, bucket_name):
    try:
        # Assume the IAM role
        sts_client = boto3.client('sts', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        response = sts_client.assume_role(RoleArn=role_arn, RoleSessionName='AssumedRoleSession')

        # Extract temporary credentials from the assumed role
        assumed_role_credentials = response['Credentials']
a
        # Create an S3 client using the assumed role's temporary credentials
        s3_assumed_role = boto3.client(
            's3',
            aws_access_key_id=assumed_role_credentials['AccessKeyId'],
            aws_secret_access_key=assumed_role_credentials['SecretAccessKey'],
            aws_session_token=assumed_role_credentials['SessionToken']
        )

        # List all files in the specified S3 bucket
        objects = s3_assumed_role.list_objects_v2(Bucket=bucket_name)

        # If the code reaches here, the credentials are valid, and the assumed role was successful
        print(f"List of files in S3 bucket '{bucket_name}':")
        for obj in objects.get('Contents', []):
            print(obj['Key'])

    except Exception as e:
        # If an exception occurs, the credentials are invalid or the assumed role failed
        print("AWS credentials with assumed role are invalid. Error:", e)

# Replace 'YOUR_ACCESS_KEY', 'YOUR_SECRET_KEY', 'ROLE_ARN', and 'BUCKET_NAME' with your actual values
access_key = '<update yours>'
secret_key = '<update yours>'
role_arn = 'arn:aws:iam::<update yours>:role/abc-s3-kms-ro-role'
bucket_name = 'abc-dev01-incoming-files'

# Check AWS credentials with assumed role and list files in the specified S3 bucket
list_files_in_bucket(access_key, secret_key, role_arn, bucket_name)
