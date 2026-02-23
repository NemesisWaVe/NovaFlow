import json
import boto3
import uuid
import os

dynamodb=boto3.resource('dynamodb')
sqs =boto3.client('sqs')

SQS_QUEUE_URL= os.environ.get('SQS_QUEUE_URL','YOUR_SQS_URL_HERE')
TABLE_NAME='NovaFlow_Tasks'

table=dynamodb.Table(TABLE_NAME)
MAX_FREE_QUESTIONS=3

def lambda_handler(event, context):
    try:
        #1 First we parse the req from API gateway
        body=json.loads(event['body'])
        user_id=body.get('user_id','demo_user')
        s3_file_key=body['file_key']
        user_prompt=body['prompt']
        tasks_id=str(uuid.uuid4())

        #2 THE VALUE FENCE: check DynamoDB quota
        # (For MVP, we just query how many tasks this user has submitted)
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('user_id').eq(user_id)
        )
        questions_asked=len(response.get('Items',[]))
        if questions_asked >= MAX_FREE_QUESTIONS:
            return {
                'statusCode': 402,
                'headers' :{'Access-Control-Allow-Origin':'*'},
                'body': json.dumps({'error':'Maximum compute quota reached.'})
            }
        #3 Create the task record (status:pending)
        table.put_item(
            Items={
                'task_id':tasks_id,
                'user_id':user_id,
                'task_status':'pending',
                'file_key':s3_file_key,
                'rompt':user_prompt,
            }
        )

        #4 Drop the job into SQS for the Nova worker
        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({
                'task_id':tasks_id,
                'file_key':s3_file_key,
                'prompt':user_prompt
            })
        )

        #5 Return success to the React UI instantly
        return {
            'statusCode': 202, # Accepted for processing
            'headers':{'Access-Control-Allow-Origin':'*'},
            'body': json.dumps({
                'task_id':tasks_id
                'status':'pending'
            })
    except Exception as e:
        return {
            'statusCode':500,
            'headers':{'Access-Control-Allow-Origin':'*'},
            'body':json.dumps({'error': str(e)})
        }