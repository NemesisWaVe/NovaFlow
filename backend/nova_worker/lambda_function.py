import json
import boto3
import csv
import io
import sqlite3
import re
import pandas as pd 
dynamodb = boto3.resource('dynamodb')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
s3 = boto3.client('s3')

TABLE_NAME = 'NovaFlow_Tasks'
S3_BUCKET_NAME = 'novaflow-data-artifacts-2026' 

def update_status(table, task_id, phase, log_message, sql=None):
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
            # PHASE 1: INGESTION & AUTONOMOUS PREPROCESSING
            update_status(table, task_id, "ingesting", "Executing Pandas preprocessing: Imputing nulls and coercing types...")
            s3_response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
            
            # 1. Load data directly into a Pandas DataFrame (Limit to 50k rows for memory safety)
            df = pd.read_csv(s3_response['Body'], nrows=50000)
            initial_rows = len(df)
            
            # 2. The Auto-Cleaner: Drop completely empty rows and columns
            df.dropna(axis=1, how='all', inplace=True)
            df.dropna(axis=0, how='all', inplace=True)
            
            # 3. Schema Normalization: Clean column headers (no spaces, all lowercase)
            df.columns = [re.sub(r'\W+', '_', str(col).strip().lower()) for col in df.columns]
            
            # 4. Smart Imputation: Fix dirty data autonomously
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    # If numbers are missing, fill them with the column's Median value
                    df[col] = df[col].fillna(df[col].median())
                else:
                    # If text is missing, label it 'Unknown' and strip trailing spaces
                    df[col] = df[col].fillna('Unknown').astype(str).str.strip()
            
            final_rows = len(df)
            preprocessing_telemetry = f"Ingested {initial_rows} rows. Dropped {initial_rows - final_rows} empty rows. Sanitized {len(df.columns)} columns. Nulls imputed via median/mode."

            # 5. Load the mathematically sound data into SQLite
            conn = sqlite3.connect(':memory:')
            df.to_sql('dataset', conn, index=False, if_exists='replace')
            cursor = conn.cursor()
            
            safe_headers = list(df.columns)
            update_status(table, task_id, "ingesting", "Dataset cleaned and loaded into memory successfully.")

            # PHASE 2: PLANNING & SQL GENERATION
            update_status(table, task_id, "planning", "Analyzing user prompt and mapping to SQLite schema...")
            schema_context = f"Table 'dataset' columns: {', '.join(safe_headers)}"
            sql_prompt = f"""
            User Request: {user_prompt}
            
            Write the SQLite queries to extract this data. You may write multiple queries separated by semicolons.
            CRITICAL SQLITE LIMITATIONS: 
            - SQLite DOES NOT support CORREL(), STDEV(), or VAR() functions. 
            - If the user asks for a correlation, you MUST work around this by grouping data into bins/ranges and calculating the AVG() of the target variable.
            
            Return ONLY raw SQL. No markdown.
            """
            sql_system = f"You are a master Data Engineer. Schema: {schema_context}. Output pure SQL. No markdown."
            raw_sql = invoke_nova(sql_prompt, sql_system).strip().replace('```sql', '').replace('```', '')
            update_status(table, task_id, "planning", "SQL generated successfully.", raw_sql)

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
                    update_status(table, task_id, "executing", f"Error in query {i+1}: {str(db_e)}", raw_sql)

            # PHASE 4: SYNTHESIS & VISUALIZATION (UPGRADED FOR ENTERPRISE CHARTS)
            update_status(table, task_id, "synthesizing", "Data extracted. Generating Plotly schemas and McKinsey-style brief...")
            final_prompt = f"""
            User Request: {user_prompt}
            Mathematical Results Extracted: {json.dumps(data_sample)}

            Act as an elite Principal Data Scientist at McKinsey. 
            You MUST output a strictly valid JSON object with EXACTLY these three keys: "main_answers", "strategy_brief", and "visualizations".

            1. "main_answers": Direct answers formatted EXACTLY like this (USE DOUBLE NEWLINES between lines):
               **Scenario [X]: [Title]**
               
               *Analysis Goal:* [1 sentence]
               
               *Key Finding:* [Use exact numbers from the data]
               
               *Insight:* [Actionable business/medical takeaway]
               
            2. "strategy_brief": An object with "descriptive", "predictive", and "prescriptive" keys. 
               CRITICAL: You MUST write at least 3 to 4 highly detailed, sophisticated sentences for each of these three keys. Do not be brief. Think deeply about the business impact.

            3. "visualizations": An array of exactly 1 to 3 chart objects.
               - AUTONOMY: Choose the MOST APPROPRIATE chart type for the data (e.g., 'line' for trends, 'bar' for comparisons, 'scatter' for distributions, 'pie' for proportions, 'heatmap' for correlation matrices).
               - FORMAT: Each object MUST be a strictly valid Plotly.js JSON configuration containing "data" and "layout".
               - RULE 1 (Comparisons): If comparing multiple categories (e.g., Male vs Female), you MUST use MULTIPLE traces in the "data" array so they are colored differently. (If using bar charts, include "barmode": "group" in layout).
               - RULE 2 (The Heatmap Trap): IF you choose to generate a heatmap, the "z" property MUST be a nested 2D matrix (array of arrays), NOT a flat 1D list. (Example: "z": [[1.5, 2.0], [3.1, 0.5]]). DO NOT output a flat 1D array for "z" or the UI will crash.
               - AESTHETICS: Provide a clear "title" and axis labels in the "layout".
            """
            final_system = "You are an elite Business Strategist. Output strictly valid JSON."
            final_response = invoke_nova(final_prompt, final_system).strip().replace('```json', '').replace('```', '')
            ai_output = json.loads(final_response)
            update_status(table, task_id, "synthesizing", "Analysis complete. Preparing visualizations...", raw_sql)

            # PHASE 5: COMPLETION
            table.update_item(
                Key={'task_id': task_id},
                UpdateExpression="SET task_status = :s, ai_analysis = :a, chart_data = :c, current_phase = :p",
                ExpressionAttributeValues={
                    ':s': 'completed',
                    ':a': json.dumps({
                        "main_answers": ai_output.get("main_answers", ""),
                        "strategy_brief": ai_output.get("strategy_brief", {}),
                        "raw_sql": raw_sql, # PASSING THE REAL SQL
                        "preprocessing_log": preprocessing_telemetry # PASSING THE CLEANING STATS
                    }),
                    ':c': json.dumps(ai_output.get("visualizations", [])),
                    ':p': 'done'
                }
            )
        except Exception as e:
            print(f"CRITICAL SYSTEM CRASH: {str(e)}")
            table.update_item(
                Key={'task_id': task_id},
                UpdateExpression="SET task_status = :s, error_msg = :e",
                ExpressionAttributeValues={':s': 'failed', ':e': str(e)}
            )

    return {'statusCode': 200, 'body': 'Batch Processed'}
