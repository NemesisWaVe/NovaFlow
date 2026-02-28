import json
import boto3
import csv
import io
import sqlite3
import re

dynamodb = boto3.resource('dynamodb')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
s3 = boto3.client('s3')

TABLE_NAME = 'NovaFlow_Tasks'
S3_BUCKET_NAME = 'novaflow-data-artifacts-2026' # <--- EXACT LOWERCASE NAME

def update_status(table, task_id, phase, log_message, sql=None):
    # This pushes LIVE updates to the frontend
    UpdateExp = "SET current_phase = :p, execution_log = :l"
    ExpVals = {':p': phase, ':l': log_message}
    if sql:
        UpdateExp += ", raw_sql = :s"
        ExpVals[':s'] = sql
        
    table.update_item(Key={'task_id': task_id}, UpdateExpression=UpdateExp, ExpressionAttributeValues=ExpVals)

def invoke_nova(prompt, system_prompt):
    payload = {
        "schemaVersion": "messages-v1",
        "messages": [{"role": "user", "content": [{"text": prompt}]}],
        "system": [{"text": system_prompt}]
    }
    res = bedrock.invoke_model(
        modelId="amazon.nova-lite-v1:0",
        contentType="application/json",
        accept="application/json",
        body=json.dumps(payload)
    )
    return json.loads(res['body'].read())['output']['message']['content'][0]['text']

def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)

    for record in event['Records']:
        body = json.loads(record['body'])
        task_id = body['task_id']
        user_prompt = body.get('prompt')
        file_key = body.get('file_key')

        try:
            # PHASE 1: INGESTION
            update_status(table, task_id, "ingesting", "Downloading dataset and building In-Memory SQL Engine...")
            s3_response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
            csv_content = s3_response['Body'].read().decode('utf-8')
            
            conn = sqlite3.connect(':memory:')
            cursor = conn.cursor()
            csv_reader = csv.reader(io.StringIO(csv_content))
            headers = next(csv_reader)
            safe_headers = [re.sub(r'\W+', '_', h.strip().lower()) for h in headers]
            
            create_table_sql = f"CREATE TABLE dataset ({', '.join([h + ' TEXT' for h in safe_headers])})"
            cursor.execute(create_table_sql)
            insert_sql = f"INSERT INTO dataset VALUES ({', '.join(['?' for _ in safe_headers])})"
            cursor.executemany(insert_sql, list(csv_reader)[:50000])
            conn.commit()

            # PHASE 2: PLANNING & SQL GENERATION
            update_status(table, task_id, "planning", "Analyzing user prompt and mapping to dataset schema to write execution roadmap...")
            schema_context = f"Table 'dataset' columns: {', '.join(safe_headers)}"
            sql_prompt = f"User Request: {user_prompt}\n\nWrite the SQLite queries to extract this data. You may write multiple queries separated by semicolons. Return ONLY raw SQL."
            sql_system = f"You are a master Data Engineer. Schema: {schema_context}. Output pure SQL. No markdown."
            raw_sql = invoke_nova(sql_prompt, sql_system).strip().replace('```sql', '').replace('```', '')

            # PHASE 3: EXECUTION
            update_status(table, task_id, "executing", "Executing dynamic SQL against data matrix...", raw_sql)
            queries = [q.strip() for q in raw_sql.split(';') if q.strip()]
            data_sample = {}
            for i, query in enumerate(queries):
                try:
                    cursor.execute(query)
                    if cursor.description:
                        col_names = [d[0] for d in cursor.description]
                        data_sample[f"Query_{i+1}"] = [dict(zip(col_names, row)) for row in cursor.fetchall()[:25]]
                except Exception as db_e:
                    data_sample[f"Query_{i+1}_Error"] = str(db_e)

            # PHASE 4: SYNTHESIS & VISUALIZATION
            update_status(table, task_id, "synthesizing", "Data extracted. Synthesizing multi-modal strategy brief and rendering visual payloads...")
            final_prompt = f"""
            User Request: {user_prompt}
            Mathematical Results Extracted: {json.dumps(data_sample)}

            Act as an elite Data Analyst. 
            Output a JSON object with EXACTLY these keys:
            1. "main_answers": Direct, detailed answers to the user's questions. USE MARKDOWN FREELY (bolding, lists, headers).
            2. "strategy_brief": An object with "descriptive", "predictive", and "prescriptive" keys. Use markdown.
            3. "visualizations": An array of 1 to 3 objects. 
               Each object must have "title", "type" (bar, line, scatter, or heatmap), and "raw_data" (an array of JSON objects representing the exact X/Y or Matrix coordinates to be plotted). DO NOT generate Chart.js configs. Just output the raw data array so the frontend can build it natively.
            """
            final_system = "You are an elite Business Strategist. Output strictly valid JSON."
            final_response = invoke_nova(final_prompt, final_system).strip().replace('```json', '').replace('```', '')
            ai_output = json.loads(final_response)

            # PHASE 5: COMPLETION
            table.update_item(
                Key={'task_id': task_id},
                UpdateExpression="SET task_status = :s, ai_analysis = :a, chart_data = :c, current_phase = :p",
                ExpressionAttributeValues={
                    ':s': 'completed',
                    ':a': json.dumps({
                        "main_answers": ai_output.get("main_answers", ""),
                        "strategy_brief": ai_output.get("strategy_brief", {})
                    }),
                    ':c': json.dumps(ai_output.get("visualizations", [])),
                    ':p': 'done'
                }
            )

        except Exception as e:
            table.update_item(
                Key={'task_id': task_id},
                UpdateExpression="SET task_status = :s, error_msg = :e",
                ExpressionAttributeValues={':s': 'failed', ':e': str(e)}
            )

    return {'statusCode': 200, 'body': 'Batch Processed'}
