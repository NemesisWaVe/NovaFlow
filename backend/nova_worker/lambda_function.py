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
            
            df = pd.read_csv(s3_response['Body'], nrows=50000)
            initial_rows = len(df)
            
            df.dropna(axis=1, how='all', inplace=True)
            df.dropna(axis=0, how='all', inplace=True)
            
            df.columns = [re.sub(r'\W+', '_', str(col).strip().lower()) for col in df.columns]
            
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].median())
                else:
                    df[col] = df[col].fillna('Unknown').astype(str).str.strip()
            
            final_rows = len(df)
            preprocessing_telemetry = f"Ingested {initial_rows} rows. Dropped {initial_rows - final_rows} empty rows. Sanitized {len(df.columns)} columns."

            try:
                corr_matrix = df.corr(numeric_only=True).round(2).to_dict()
                preprocessing_telemetry += " Pearson matrix computed."
            except Exception:
                corr_matrix = {"error": "Could not compute correlation matrix"}

            conn = sqlite3.connect(':memory:')
            df.to_sql('dataset', conn, index=False, if_exists='replace')
            cursor = conn.cursor()
            
            safe_headers = list(df.columns)
            update_status(table, task_id, "ingesting", "Dataset cleaned and loaded into memory successfully.")

            # --- ENTERPRISE QUERY REFORMULATION (THE ROUTER) ---
            update_status(table, task_id, "planning", "Extracting core intent from conversation history...")
            intent_prompt = f"Read this input: {user_prompt}\n\nReformulate the user's FINAL request into a single, highly specific standalone instruction. Do not answer the request, just state what data or analysis they want right now."
            clean_intent = invoke_nova(intent_prompt, "You are an intent extraction router. Output a single clear sentence.").strip()

            # PHASE 2: PLANNING & SQL GENERATION
            update_status(table, task_id, "planning", "Mapping clean intent to SQLite schema...")
            schema_context = f"Table 'dataset' columns: {', '.join(safe_headers)}"
            
            sql_system = f"""You are an elite Data Engineer writing SQLite queries.
            
            AVAILABLE SCHEMA:
            {schema_context}
            
            ABSOLUTE STRICT RULES:
            1. ONLY use the exact column names listed in the SCHEMA above. If a column isn't there, do NOT use it.
            2. SQLite DOES NOT support CORREL(), STDEV(), VAR(), or pearson_corr(). You MUST use simple math or GROUP BY averages to approximate relationships.
            3. Output pure SQL only. No explanations. No markdown formatting.
            """
            
            sql_prompt = f"Write 1 to 3 SQLite queries separated by semicolons to accomplish this specific task: {clean_intent}"
            
            raw_sql = invoke_nova(sql_prompt, sql_system).strip().replace('```sql', '').replace('```', '')
            update_status(table, task_id, "planning", "SQL generated successfully.", raw_sql)

            # PHASE 3: EXECUTION (DECOUPLED DATA PIPELINE)
            update_status(table, task_id, "executing", "Executing dynamic SQL against data matrix...", raw_sql)
            
            # Limit to 3 queries max to prevent AI hallucination
            queries = [q.strip() for q in raw_sql.split(';') if q.strip()][:3] 
            
            full_data_sample = {} # Goes to Charts
            ai_data_sample = {}   # Goes to AI Prompt
            
            for i, query in enumerate(queries):
                try:
                    cursor.execute(query)
                    if cursor.description:
                        col_names = [d[0] for d in cursor.description]
                        all_rows = cursor.fetchall()
                        
                        # Charts get everything
                        full_data_sample[f"Query_{i+1}"] = [dict(zip(col_names, row)) for row in all_rows]
                        # AI only gets top 3 rows to save tokens
                        ai_data_sample[f"Query_{i+1}"] = [dict(zip(col_names, row)) for row in all_rows[:3]]
                except Exception as db_e:
                    ai_data_sample[f"Query_{i+1}_Error"] = str(db_e)
                    update_status(table, task_id, "executing", f"Error in query {i+1}: {str(db_e)}", raw_sql)

            # PHASE 4: THE DYNAMIC ORCHESTRATOR & 5-PILLAR STRATEGY
            import plotly.express as px
            import plotly.graph_objects as go
            
            update_status(table, task_id, "synthesizing", "Generating 5-Pillar Strategy Brief and dynamic visualizations...")
            
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            feature_blacklist = ['patient_id', 'member_id', 'year_received', 'birth_year', 'record_index']
            clean_feature_cols = [col for col in numeric_cols if col.lower() not in feature_blacklist]
            
            query_keys = list(ai_data_sample.keys())

            prompt_strategy = f"""
            User's Cleaned Request: {clean_intent}
            
            <sql_results>
            {json.dumps(ai_data_sample)}
            </sql_results>
            
            You are a Principal Data Scientist. Answer the user's request based on the <sql_results>.
            
            Output a STRICTLY VALID JSON object (NOT AN ARRAY) with exactly two keys: "strategy_brief" and "point_analyses".
            
            {{
                "strategy_brief": {{
                    "diagnostic": "Assess data health.",
                    "descriptive": "What is happening currently?",
                    "predictive": "Where is this trend heading?",
                    "prescriptive": "What actionable steps should be taken?",
                    "limitations": "What are the blind spots?"
                }},
                "point_analyses": [
                    {{
                        "point_id": "point_1",
                        "point_title": "Insight Title",
                        "point_answers": "Rich **markdown** with bullet points.",
                        "chart_type": "bar",
                        "x_col": "{clean_feature_cols[0] if clean_feature_cols else 'x'}",
                        "y_col": "MUST BE A NUMERIC COLUMN",
                        "color_col": null
                    }}
                ]
            }}
            """
            sys_strategy = "You are a JSON formatter. Output ONLY valid JSON. Keep text concise."
            res_strategy = invoke_nova(prompt_strategy, sys_strategy).strip().replace('```json', '').replace('```', '')
            
            # --- THE ULTIMATE JSON ARMOR ---
            point_analyses = []
            strategy_brief = {}
            try:
                parsed_res = json.loads(res_strategy, strict=False)
                
                # If AI hallucinates an array for the root object, extract the dictionary inside it
                if isinstance(parsed_res, list):
                    parsed_res = parsed_res[0] if len(parsed_res) > 0 else {}
                    
                strategy_brief = parsed_res.get("strategy_brief", {})
                point_analyses = parsed_res.get("point_analyses", [])
                
                # If AI hallucinates a dict instead of an array for point_analyses, wrap it
                if isinstance(point_analyses, dict):
                    point_analyses = [point_analyses]
                    
            except Exception as e:
                print(f"JSON Parse Error: {e}. Raw: {res_strategy}")
                strategy_brief = {"descriptive": "Data analysis complete, but narrative generation failed due to formatting limits.", "prescriptive": "Please try asking a narrower question."}
                point_analyses = [{"point_id": "error", "point_title": "Analysis Processed", "point_answers": "Data extracted successfully. View charts for insights.", "chart_type": "heatmap"}]

            # --- DETERMINISTIC CHART ENGINE ---
            linked_chart_jsons = []
            
            for index, point in enumerate(point_analyses):
                c_json = {}
                p_id = point.get("point_id", f"point_{index}")
                p_title = point.get("point_title", "Data View")
                c_type = point.get("chart_type", "bar")
                
                try:
                    if c_type == "heatmap":
                        df_corr = df[clean_feature_cols].corr().round(2)
                        fig = px.imshow(df_corr, text_auto='.2f', aspect='auto', color_continuous_scale='RdBu_r')
                    
                    else:
                        # Feed the FULL dataset to the charts, not the truncated AI preview
                        query_key = f"Query_{index + 1}"
                        query_data = full_data_sample.get(query_key, full_data_sample.get('Query_1', []))
                        df_chart = pd.DataFrame(query_data)
                        
                        x_col = point.get("x_col")
                        y_col = point.get("y_col")
                        color_col = point.get("color_col")
                        
                        if df_chart.empty:
                            df_chart = pd.DataFrame({"x": ["No Data"], "y": [0]})
                            x_col, y_col, color_col = "x", "y", None
                        else:
                            if x_col not in df_chart.columns: x_col = df_chart.columns[0]
                            if y_col not in df_chart.columns: y_col = df_chart.columns[1] if len(df_chart.columns) > 1 else df_chart.columns[0]
                            if color_col not in df_chart.columns: color_col = None

                            if not pd.api.types.is_numeric_dtype(df_chart[y_col]):
                                num_cols = df_chart.select_dtypes(include='number').columns
                                if len(num_cols) > 0:
                                    y_col = num_cols[0]

                        if c_type == "scatter":
                            fig = px.scatter(df_chart, x=x_col, y=y_col, color=color_col)
                            fig.update_traces(marker=dict(size=10)) 
                        else:
                            fig = px.bar(df_chart, x=x_col, y=y_col, color=color_col, barmode='group')
                    
                    fig.update_layout(
                        template='plotly_dark', 
                        paper_bgcolor='rgba(0,0,0,0)', 
                        plot_bgcolor='rgba(0,0,0,0)',
                        title='',
                        margin=dict(t=20, b=50, l=50, r=20)
                    )
                    c_json = json.loads(fig.to_json())
                    c_json["meta"] = {"scenario_id": p_id}
                    
                except Exception as chart_e:
                    print(f"Chart Failed on {p_id}: {chart_e}")
                    fig_e = px.bar(x=["Error"], y=[0])
                    fig_e.update_layout(template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                    c_json = json.loads(fig_e.to_json())
                    c_json["meta"] = {"scenario_id": p_id}

                linked_chart_jsons.append(c_json)

            # PHASE 5: COMPLETION
            table.update_item(
                Key={'task_id': task_id},
                UpdateExpression="SET task_status = :s, ai_analysis = :a, chart_data = :c, current_phase = :p",
                ExpressionAttributeValues={
                    ':s': 'completed',
                    ':a': json.dumps({
                        "strategy_brief": strategy_brief,
                        "point_analyses": point_analyses,
                        "raw_sql": raw_sql,
                        "preprocessing_log": preprocessing_telemetry
                    }),
                    ':c': json.dumps(linked_chart_jsons), 
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
