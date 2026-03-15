import json
import boto3
import uuid
import os
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')
s3 = boto3.client('s3')

TABLE_NAME = os.environ.get('TABLE_NAME', 'NovaFlow_Tasks')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'novaflow-data-artifacts-2026')

def build_response(status_code, body):
    """Helper utility to ensure consistent CORS headers across all responses."""
    return {
        'statusCode': status_code,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            'Access-Control-Allow-Headers': 'Content-Type'
        },
        'body': json.dumps(body)
    }

def lambda_handler(event, context):
    try:
        table = dynamodb.Table(TABLE_NAME)
        body = json.loads(event.get('body', '{}'))
        action = body.get('action')

        # Route: Delete Workspace Task
        if action == 'delete_task':
            task_id = body.get('task_id')
            if task_id:
                table.delete_item(Key={'task_id': task_id})
            return build_response(200, {'status': 'deleted'})

        # Route: Secure S3 Direct Upload
        if action == 'get_upload_url':
            file_name = body.get('file_name', 'data.csv')
            file_key = f"uploads/{str(uuid.uuid4())[:8]}_{file_name}"
            
            presigned_url = s3.generate_presigned_url(
                ClientMethod='put_object',
                Params={
                    'Bucket': S3_BUCKET_NAME,
                    'Key': file_key,
                    'ContentType': 'text/csv'
                },
                ExpiresIn=300
            )
            return build_response(200, {'upload_url': presigned_url, 'file_key': file_key})

        # Route: Fetch User Workspace History
        if action == 'get_history':
            user_id = body.get('user_id', 'anonymous_guest')
            response = table.scan(FilterExpression=Attr('user_id').eq(user_id))
            items = response.get('Items', [])
            
            history_list = [
                {
                    'task_id': item.get('task_id'),
                    'prompt_snippet': item.get('prompt', 'Analysis Task')[:45] + '...',
                    'timestamp': item.get('last_updated', 'Recent')
                }
                for item in items if item.get('task_status') == 'completed'
            ]
            
            history_list = sorted(history_list, key=lambda x: x['timestamp'], reverse=True)
            return build_response(200, {'history': history_list})
                
        # Route: Asynchronous Polling Interceptor
        if action == 'check_status':
            task_id = body.get('task_id')
            response = table.get_item(Key={'task_id': task_id})
            
            if 'Item' in response:
                item = response['Item']
                return build_response(200, {
                    'task_status': item.get('task_status', 'processing'),
                    'current_phase': item.get('current_phase', 'processing'),
                    'ai_analysis': item.get('ai_analysis', '{}'),
                    'chart_data': item.get('chart_data', '[]'),
                    'error_msg': item.get('error_msg', '')
                })
            return build_response(404, {'error': 'Task not found'})

        # Route: Task Ingestion & Rate Limiting
        user_id = body.get('user_id', 'anonymous_guest')
        user_email = body.get('email', None)
        s3_file_key = body.get('file_key', 'unknown.csv')
        user_prompt = body.get('prompt', '')
        task_id = str(uuid.uuid4())
        
        # Enforce usage quotas (Feature flag set to high for demo environments)
        response = table.scan(FilterExpression=Attr('user_id').eq(user_id))
        questions_asked = len(response.get('Items', []))
        max_allowed = int(os.environ.get('MAX_QUOTA', 9999))

        if questions_asked >= max_allowed:
            return build_response(402 if user_email else 403, {'error': 'Quota exceeded', 'limit': max_allowed})

        # Initialize state
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

        # Dispatch to Worker Queue
        sqs_message = {'task_id': task_id, 'prompt': user_prompt, 'file_key': s3_file_key}
        sqs.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(sqs_message))

        return build_response(202, {'task_id': task_id, 'status': 'pending'})

    except Exception as e:
        return build_response(500, {'error': str(e)})
