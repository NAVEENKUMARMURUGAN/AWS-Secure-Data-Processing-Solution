import json
import boto3

client = boto3.client('ses', region_name='ap-southeast-2')

def lambda_handler(event, context):
    
    print(event)
    
    # Assuming 'errorDetails' and 'pipeline' are strings
    error_details = event["body"]
    pipeline = event["load"]
    
    # Format the email body using HTML for color and formatting
    html_body = f'''
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    background-color: #f4f4f4;
                    color: #333;
                }}
                .container {{
                    max-width: 600px;
                    margin: 0 auto;
                    padding: 20px;
                    background-color: #fff;
                    border-radius: 10px;
                    box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
                }}
                .error-details {{
                    color: #ff0000;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2 style="color: #007bff;">Failure - Pipeline: {pipeline}</h2>
                <p class="error-details">Error Details: {error_details}</p>
            </div>
        </body>
        </html>
    '''

    response = client.send_email(
        Destination={
            'ToAddresses': ['mndlsoft@gmail.com']
        },
        Message={
            'Body': {
                'Html': {
                    'Charset': 'UTF-8',
                    'Data': html_body
                }
            },
            'Subject': {
                'Charset': 'UTF-8',
                'Data': 'Failure - Pipeline: ' + pipeline
            },
        },
        Source='mndlsoft@gmail.com'
    )
    
    print(response)
    
    return {
        'statusCode': 200,
        'body': json.dumps("Email Sent Successfully. MessageId is: " + response['MessageId'])
    }
