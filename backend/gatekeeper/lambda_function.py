import json
import boto3
import uuid
import os
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')
s3 = boto3.client('s3')

TABLE_NAME = 'NovaFlow_Tasks'
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

# 👇 CHANGE THIS TO YOUR EXACT S3 BUCKET NAME!
S3_BUCKET_NAME = 'novaflow-data-artifacts-2026' 

def lambda_handler(event, context):
    try:
        table = dynamodb.Table(TABLE_NAME)
        body = json.loads(event.get('body', '{}'))
        action = body.get('action')

        # --- 1. NEW: PRESIGNED URL GENERATOR ---
        if action == 'get_upload_url':
            file_name = body.get('file_name', 'data.csv')
            
            # Generate a secure, unique file path so users don't overwrite each other's CSVs
            file_key = f"uploads/{str(uuid.uuid4())[:8]}_{file_name}"
            
            # Create the 5-minute VIP Pass
            presigned_url = s3.generate_presigned_url(
                ClientMethod='put_object',
                Params={
                    'Bucket': S3_BUCKET_NAME,
                    'Key': file_key,
                    'ContentType': 'text/csv'
                },
                ExpiresIn=300
            )
            
            return {
                'statusCode': 200,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({
                    'upload_url': presigned_url, 
                    'file_key': file_key
                })
            }

        # --- 2. EXISTING: POLLING INTERCEPTOR ---
        if action == 'check_status':
                task_id = body.get('task_id')
                response = table.get_item(Key={'task_id': task_id})
                
                if 'Item' in response:
                    item = response['Item']
                    return {
                        'statusCode': 200,
                        'headers': {
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Headers': 'Content-Type',
                            'Access-Control-Allow-Methods': 'OPTIONS,POST'
                        },
                        'body': json.dumps({
                            'task_status': item.get('task_status', 'processing'),
                            'current_phase': item.get('current_phase', 'processing'),
                            'ai_analysis': item.get('ai_analysis', '{}'),
                            'chart_data': item.get('chart_data', '[]'), # THE CRITICAL NEW KEY
                            'error_msg': item.get('error_msg', '')
                        })
                    }
                else:
                    return {
                        'statusCode': 404,
                        'body': json.dumps({'error': 'Task not found'})
                    }

        # --- 3. EXISTING: PLG PAYWALL & SQS LOGIC ---
        user_id = body.get('user_id', 'anonymous_guest')
        user_email = body.get('email', None)
        s3_file_key = body.get('file_key', 'unknown.csv')
        user_prompt = body.get('prompt', '')
        task_id = str(uuid.uuid4())

        # Check Quota
        response = table.scan(FilterExpression=Attr('user_id').eq(user_id))
        questions_asked = len(response.get('Items', []))
        max_allowed = 5 if user_email else 3

        if questions_asked >= max_allowed:
            status_code = 402 if user_email else 403
            return {
                'statusCode': status_code,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'Quota exceeded', 'limit': max_allowed})
            }

        # Log Task
        item_to_insert = {
            'task_id': task_id,
            'user_id': user_id,
            'task_status': 'pending',
            'file_key': s3_file_key,
            'prompt': user_prompt
        }
        if user_email:
            item_to_insert['user_email'] = user_email

        table.put_item(Item=item_to_insert)

        # Drop to SQS (Now passing the file_key so the Worker can find the CSV!)
        sqs_message = {'task_id': task_id, 'prompt': user_prompt, 'file_key': s3_file_key}
        sqs.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(sqs_message))

        return {
            'statusCode': 202,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'task_id': task_id, 'status': 'pending'})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }
