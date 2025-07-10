import json
import os
import boto3
import uuid
from datetime import datetime
import urllib.parse

# AWS Region
REGION = 'your_region'

# Initialize AWS clients with region
s3 = boto3.client('s3', region_name=REGION)
textract = boto3.client('textract', region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
ses = boto3.client('ses', region_name=REGION)

# Environment variables
email = 'Your_email'
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'Bills')
SES_SENDER_EMAIL = os.environ.get('SES_SENDER_EMAIL', email)
SES_RECIPIENT_EMAIL = os.environ.get('SES_RECIPIENT_EMAIL', email)

def lambda_handler(event, context):
    for record in event.get('Records', []):
        try:
            bucket = record['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(record['s3']['object']['key'])

            print(f"Processing bill from {bucket}/{key}")

            s3.head_object(Bucket=bucket, Key=key)

            # Step 1: Extract using Textract
            bill_data = process_bill_with_textract(bucket, key)

            # Step 2: Store in DynamoDB
            store_bill_in_dynamodb(bill_data)

            # Step 3: Email Notification
            send_email_notification(bill_data)

        except Exception as e:
            print(f"Error processing record: {str(e)}")

    return {
        'statusCode': 200,
        'body': json.dumps('Bill(s) processed successfully!')
    }

def process_bill_with_textract(bucket, key):
    try:
        response = textract.analyze_expense(
            Document={'S3Object': {'Bucket': bucket, 'Name': key}}
        )

        text_response = textract.detect_document_text(
            Document={'S3Object': {'Bucket': bucket, 'Name': key}}
        )

    except Exception as e:
        raise Exception(f"Textract failed: {str(e)}")

    bill_id = str(uuid.uuid4())
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    time_str = now.strftime('%H:%M:%S')

    bill_data = {
        'Bill_id': bill_id,
        'bill_date': date_str,
        'bill_time': time_str,
        'Service_name': 'Unknown',
        'Total_amount': '0.00',
        'items': [],
    }

    if 'ExpenseDocuments' in response and response['ExpenseDocuments']:
        doc = response['ExpenseDocuments'][0]

        for field in doc.get('SummaryFields', []):
            field_type = field.get('Type', {}).get('Text', '')
            value = field.get('ValueDetection', {}).get('Text', '')

            if field_type == 'TOTAL':
                bill_data['Total_amount'] = value
            elif field_type == 'INVOICE_RECEIPT_DATE':
                bill_data['bill_date'] = value

        for group in doc.get('LineItemGroups', []):
            for line_item in group.get('LineItems', []):
                item = {}
                for field in line_item.get('LineItemExpenseFields', []):
                    f_type = field.get('Type', {}).get('Text', '')
                    val = field.get('ValueDetection', {}).get('Text', '')
                    if f_type == 'ITEM':
                        item['name'] = val
                    elif f_type == 'PRICE':
                        item['price'] = val
                    elif f_type == 'QUANTITY':
                        item['quantity'] = val
                if 'name' in item:
                    bill_data['items'].append(item)

    # Extract first 2 lines of text for Service_name
    if text_response and 'Blocks' in text_response:
        lines = [b['Text'] for b in text_response['Blocks'] if b['BlockType'] == 'LINE']
        if len(lines) >= 2:
            bill_data['Service_name'] = f"{lines[0]}\n{lines[1]}"
        elif len(lines) == 1:
            bill_data['Service_name'] = lines[0]

    print(f"Extracted bill data: {json.dumps(bill_data)}")
    return bill_data

def store_bill_in_dynamodb(bill_data):
    try:
        table = dynamodb.Table(DYNAMODB_TABLE)

        db_item = {
            'Bill_id': bill_data['Bill_id'],
            'bill_date': bill_data['bill_date'],
            'bill_time': bill_data['bill_time'],
            'Service_name': bill_data['Service_name'],
            'Total_amount': bill_data['Total_amount'],
            'items': bill_data['items'],
            'processed_timestamp': datetime.now().isoformat()
        }

        table.put_item(Item=db_item)
        print(f"Bill stored in DynamoDB: {bill_data['Bill_id']}")
    except Exception as e:
        raise Exception(f"DynamoDB Error: {str(e)}")

def send_email_notification(bill_data):
    try:
        items_html = ""
        for item in bill_data['items']:
            name = item.get('name', 'Unknown')
            price = item.get('price', '0.00')
            quantity = item.get('quantity', '1')
            items_html += f"<li>{name} - ₹{price} x {quantity}</li>"

        if not items_html:
            items_html = "<li>No items found</li>"

        html_body = f"""
        <html>
        <body>
            <h2>{bill_data['Service_name']}</h2>
            <p><strong>Date:</strong> {bill_data['bill_date']}</p>
            <p><strong>Time:</strong> {bill_data['bill_time']}</p>
            <p><strong>Total:</strong> ₹{bill_data['Total_amount']}</p>
            <h3>Items:</h3>
            <ul>{items_html}</ul>
        </body>
        </html>
        """

        ses.send_email(
            Source=email,
            Destination={
                'ToAddresses': [email]
            },
            Message={
                'Subject': {
                    'Data': f"Bill Processed: ₹{bill_data['Total_amount']}"
                },
                'Body': {
                    'Html': {
                        'Data': html_body
                    }
                }
            }
        )

        print(f"Email sent to {email}")
    except Exception as e:
        print(f"Email send error: {str(e)}")
