import json
import boto3
import base64
import urllib.request
import urllib.parse
import csv
import io
import sqlite3
import re

dynamodb = boto3.resource('dynamodb')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
s3 = boto3.client('s3')

TABLE_NAME = 'NovaFlow_Tasks'
S3_BUCKET_NAME = 'novaflow-data-artifacts-2026' # <--- EXACT LOWERCASE NAME HERE

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
    body = json.loads(res['body'].read())
    return body['output']['message']['content'][0]['text']

def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)

    for record in event['Records']:
        body = json.loads(record['body'])
        task_id = body['task_id']
        user_prompt = body.get('prompt', 'Analyze the dataset.')
        file_key = body.get('file_key')

        try:
            # 1. Ingest Data to SQLite
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

            # 2. AGENT 1: The Quant (SQL Generation)
            schema_context = f"Table 'dataset' columns: {', '.join(safe_headers)}"
            sql_prompt = f"""
            User Request: {user_prompt}
            
            Write the SQLite queries to extract the necessary data for a deep business analysis. 
            Include aggregations or trend checks if necessary. You may write multiple queries separated by semicolons. 
            Return ONLY the raw SQL. Do not explain.
            """
            sql_system = f"You are a master Data Engineer. Schema: {schema_context}. Output pure SQL. No markdown."
            
            raw_sql = invoke_nova(sql_prompt, sql_system)
            raw_sql = raw_sql.strip().replace('```sql', '').replace('```', '')

            # 3. TOOL EXECUTION: Run the SQL
            try:
                queries = [q.strip() for q in raw_sql.split(';') if q.strip()]
                data_sample = {}
                
                for i, query in enumerate(queries):
                    cursor.execute(query)
                    if cursor.description: 
                        col_names = [description[0] for description in cursor.description]
                        data_sample[f"Query_{i+1}"] = [dict(zip(col_names, row)) for row in cursor.fetchall()[:15]]
            except Exception as db_e:
                data_sample = {"Error": f"SQL Execution Failed: {str(db_e)}", "Attempted_SQL": raw_sql}

            # 4. AGENT 2: The Partner (Deep Synthesis)
            # 4. AGENT 2: The Partner (Deep Synthesis & Multi-Artifacts)
            final_prompt = f"""
            User Request: {user_prompt}
            SQL Queries Executed: {raw_sql}
            Mathematical Results Extracted: {json.dumps(data_sample)}

            Act as an elite Data Analyst. The user has asked complex, multi-part questions.
            Output a JSON object with EXACTLY these keys:
            
            1. "main_answers": A detailed string directly answering the user's specific scenarios based on the hard data. 
            2. "strategy_brief": An object with "descriptive", "predictive", and "prescriptive" keys for the sidebar.
            3. "execution_log": The actual SQL queries you ran to get this data.
            4. "visualizations": An ARRAY of objects. Create 1 to 3 charts depending on how many scenarios the user asked.
               Each object in the array must have:
               - "title": A string title for the chart (e.g., "Scenario 1: Sugar vs Activity").
               - "config": A valid QuickChart.io JSON configuration (type, data, options). 
                 CRITICAL RULES: Use ONLY "bar", "line", or "scatter". You MUST include "options": {{"plugins": {{"legend": {{"labels": {{"color": "#fff"}} }}}}, "scales": {{"xAxes": [{{"ticks": {{"fontColor": "#fff"}} }}], "yAxes": [{{"ticks": {{"fontColor": "#fff"}} }}] }} }}

            Output ONLY valid JSON. No markdown.
            """
            final_system = "You are an elite Business Strategist. Output strictly valid JSON."

            final_response = invoke_nova(final_prompt, final_system)
            final_response = final_response.strip().replace('```json', '').replace('```', '')
            
            ai_output = json.loads(final_response)
            
            # Extract the rich payload
            analysis_payload = {
                "main_answers": ai_output.get("main_answers", "No direct answers generated."),
                "strategy_brief": ai_output.get("strategy_brief", {}),
                "execution_log": ai_output.get("execution_log", raw_sql)
            }
            
            # 5. Process Multiple Charts Dynamically
            charts_data = []
            for viz in ai_output.get("visualizations", []):
                chart_config = viz.get("config", {})
                chart_title = viz.get("title", "Data Visualization")
                # Generate a direct URL instead of Base64 to save DB space
                chart_url = "https://quickchart.io/chart?w=700&h=400&bkg=transparent&c=" + urllib.parse.quote(json.dumps(chart_config))
                charts_data.append({
                    "title": chart_title,
                    "url": chart_url
                })

            # If the AI failed to make a chart, provide a fallback so it doesn't crash
            if not charts_data:
                charts_data = [{"title": "No Chart Generated", "url": ""}]

            # 6. Update DynamoDB
            table.update_item(
                Key={'task_id': task_id},
                UpdateExpression="SET task_status = :s, ai_analysis = :a, chart_data = :c",
                ExpressionAttributeValues={
                    ':s': 'completed',
                    ':a': json.dumps(analysis_payload),
                    ':c': json.dumps(charts_data) # Saving the array of chart URLs!
                }
            )

        except Exception as e:
            table.update_item(
                Key={'task_id': task_id},
                UpdateExpression="SET task_status = :s, error_msg = :e",
                ExpressionAttributeValues={':s': 'failed', ':e': str(e)}
            )

    return {'statusCode': 200, 'body': 'Batch Processed'}
