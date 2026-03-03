import json
import boto3
import sqlite3
import re
import pandas as pd
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
s3 = boto3.client('s3')

TABLE_NAME = 'NovaFlow_Tasks'
S3_BUCKET_NAME = 'novaflow-data-artifacts-2026'

def update_status(table, task_id, phase, log_message, sql=None, execution_log_append=""):
    try:
        response = table.get_item(Key={'task_id': task_id})
        current_logs = response.get('Item', {}).get('execution_log', '')
        if execution_log_append:
            log_message = f"{current_logs}\n{execution_log_append}" if current_logs else execution_log_append
    except Exception:
        pass

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

# THE CITADEL: Autonomous Data Profiler

def autonomous_data_profiler(df):
    ontology_map = {}
    df.dropna(axis=1, how='all', inplace=True)
    df.dropna(axis=0, how='all', inplace=True)
    df.columns = [re.sub(r'\W+', '_', str(col).strip().lower()) for col in df.columns]
    
    for col in df.columns:
        col_meta = {}
        if df[col].dtype == 'object':
            try:
                df[col] = pd.to_datetime(df[col])
            except (ValueError, TypeError):
                pass

        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].median())
            col_meta['type'] = 'numeric'
            col_meta['min'] = round(float(df[col].min()), 2)
            col_meta['max'] = round(float(df[col].max()), 2)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            col_meta['type'] = 'datetime'
        else:
            df[col] = df[col].fillna('Unknown').astype(str).str.strip()
            unique_count = df[col].nunique()
            if unique_count <= 20:
                col_meta['type'] = 'categorical'
                col_meta['allowed_values'] = df[col].unique().tolist()
            else:
                col_meta['type'] = 'text'
        ontology_map[col] = col_meta
    return df, ontology_map

# SQL CLEANER UTILITY

def clean_sql(raw_text):
    match = re.search(r'```(?:sql)?\n?(.*?)```', raw_text, re.DOTALL | re.IGNORECASE)
    if match:
        raw_text = match.group(1)
    match = re.search(r'(?i)(SELECT\s+.*)', raw_text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return raw_text.strip()

# AGENT 1: The Autonomous Prompt Rewriter
def compress_memory(task_id, new_prompt, table):
    response = table.get_item(Key={'task_id': task_id})
    history = response.get('Item', {}).get('conversation_history', '[]')
    
    if history == '[]':
        return new_prompt # First query needs no rewriting

    try:
        history_list = json.loads(history)
    except:
        history_list = []

    context_str = ""
    for msg in history_list[-4:]:
        role = msg.get("role", "user").upper()
        content = msg.get("content", "")
        # Cap context to prevent bloat, but keep enough for Task references
        if len(content) > 1500:
            content = content[:1500] + "... [TRUNCATED]"
        context_str += f"{role}: {content}\n"

    # THE MAGIC: We force the AI to write a standalone prompt
    rewriter_sys = """You are an AI Context Manager. Read the Chat History and the User's New Command.
    1. If the New Command is a follow-up (e.g., 'what about task 2?'), rewrite it into a fully complete, standalone command pulling the exact details of Task 2 from the history.
    2. If the New Command is completely unrelated to the history, simply output the New Command as-is.
    DO NOT answer the command. ONLY output the synthesized, standalone command string."""
    
    rewriter_prompt = f"CHAT HISTORY:\n{context_str}\n\nUSER'S NEW COMMAND: {new_prompt}\n\nREWRITTEN STANDALONE COMMAND:"
    
    standalone_prompt = invoke_nova(rewriter_prompt, rewriter_sys).strip()
    return standalone_prompt

# AGENT 3A & 3B: SQL Engine & Critic Loop
def generate_sql(system_state, ontology_map):
    sql_system = f"""You are an elite Data Engineer writing SQLite queries.
    
    DATABASE CONTEXT:
    - Table Name: `dataset` (MUST USE 'FROM dataset')
    
    ONTOLOGY MAP (STRICT SCHEMA):
    {json.dumps(ontology_map, indent=2)}
    
    CRITICAL RULES:
    1. READ THE 'CURRENT COMMAND'. 
    2. If the CURRENT COMMAND asks for a previous task (like "Task 2"), DO NOT output the old SQL. You MUST write a BRAND NEW query specifically for that task.
    3. Ignore the SQL from the RECENT CHAT LOG.
    4. To bin ages: CASE WHEN age < 40 THEN 'Under 40' WHEN age <= 60 THEN '40-60' ELSE 'Over 60' END
    5. START DIRECTLY WITH 'SELECT'. No markdown.
    """
    sql_prompt = f"Write 1 to 3 SQLite queries separated by semicolons for this task:\n{system_state}"
    raw = invoke_nova(sql_prompt, sql_system)
    return clean_sql(raw)

def evaluate_and_fix_sql(cursor, initial_sql, ontology_map, task_id, table):
    max_retries = 3
    current_sql = initial_sql
    
    for attempt in range(max_retries):
        queries = [q.strip() for q in current_sql.split(';') if q.strip()][:3]
        results = {}
        error_encountered = None
        
        for i, query in enumerate(queries):
            try:
                cursor.execute(query)
                if cursor.description:
                    col_names = [d[0] for d in cursor.description]
                    all_rows = cursor.fetchall()
                    results[f"Query_{i+1}"] = {
                        "full": [dict(zip(col_names, row)) for row in all_rows],
                        "preview": [dict(zip(col_names, row)) for row in all_rows[:3]]
                    }
            except Exception as e:
                error_encountered = str(e)
                break 
                
        if not error_encountered:
            return results, current_sql
            
        update_status(table, task_id, "executing", f"SQL Failed. Triggering Critic... (Attempt {attempt+1}/{max_retries})", current_sql, execution_log_append=f"Error: {error_encountered}")
        
        critic_sys = f"""You are a Database Critic. The previous SQL threw a fatal error.
        TABLE NAME: `dataset` (MUST use 'FROM dataset')
        ONTOLOGY MAP: {json.dumps(ontology_map)}
        ERROR THROWN: {error_encountered}
        BROKEN SQL: {current_sql}
        
        Rewrite the SQL. 
        CRITICAL: Query 'FROM dataset'. Use ONLY valid columns from the Map. 
        START DIRECTLY WITH 'SELECT'. No apologies, no markdown."""
        
        raw = invoke_nova("Rewrite the broken SQL perfectly.", critic_sys)
        current_sql = clean_sql(raw)
    
    return {"Query_1": {"full": [{"error": f"Failed: {error_encountered}"}], "preview": [{"error": "Failed"}]}}, current_sql

# AGENT 4: The Strategy Synthesizer

def synthesize_ontology(ai_data_sample, system_state, clean_feature_cols, correlation_matrix):
    prompt_strategy = f"""
    {system_state}
    
    SQL Results: {json.dumps(ai_data_sample)}
    Actual Pearson Correlation Matrix: {json.dumps(correlation_matrix)}
    
    You are a Principal Data Scientist. Answer the 'CURRENT COMMAND' based strictly on the data provided.
    
    CRITICAL LOGIC RULES:
    1. FOCUS ONLY ON THE 'CURRENT COMMAND'. 
    2. BE EXTREMELY QUANTITATIVE. Never use vague words like 'significantly higher' or 'correlates strongly'. You MUST quote the exact numerical averages, thresholds, or correlation coefficients directly from the SQL Results!
    3. Look at the Correlation Matrix for predictors (closest to 1.0 or -1.0) and state the exact 'r' value.
    4. Create actionable "Rules of Thumb" (Heuristics) for the user. Example: "If BMI is normal but Waist is over X cm, check Insulin." (Fill in X with the real data).
    
    Output a STRICTLY VALID JSON object with exactly two keys: "strategy_brief" and "point_analyses".
    Valid chart_types: ANY valid Plotly Express type (e.g., "bar", "scatter", "line", "box", "violin", "histogram", "area", "pie", "heatmap").
    
    {{
        "strategy_brief": {{
            "diagnostic": "[Write 2 real sentences assessing data health]",
            "descriptive": "[Write 2 real sentences describing current trends]",
            "predictive": "[Write 2 real sentences predicting future outcomes]",
            "prescriptive": "[Write 2 real sentences with actionable steps]",
            "limitations": "[Write 2 real sentences noting statistical blind spots]"
        }},
        "point_analyses": [
            {{
                "point_id": "point_1", 
                "point_title": "[Write Real Title Here]", 
                "point_answers": "[Write real markdown bullet points answering the CURRENT COMMAND]", 
                "chart_type": "bar", 
                "x_col": "{clean_feature_cols[0] if clean_feature_cols else 'x'}", 
                "y_col": "NUMERIC_COLUMN_NAME", 
                "color_col": "NAME_OF_CATEGORICAL_COLUMN (DO NOT leave null if comparing groups. e.g. 'risk_category')"
            }}
        ]
    }}
    """
    return invoke_nova(prompt_strategy, "Output ONLY valid JSON. Replace all bracketed placeholders with real analysis.").strip().replace('```json', '').replace('```', '')

# MAIN LAMBDA HANDLER
def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)

    for record in event['Records']:
        body = json.loads(record['body'])
        task_id = body['task_id']
        user_prompt = body.get('prompt')
        file_key = body.get('file_key')

        try:
            update_status(table, task_id, "planning", "Initializing Memory Compressor...")
            system_state = compress_memory(task_id, user_prompt, table)

            update_status(table, task_id, "ingesting", "Executing Deep Data Profiling & Ontology Mapping...")
            s3_response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
            df = pd.read_csv(s3_response['Body'], nrows=50000)
            
            df, ontology_map = autonomous_data_profiler(df)
            preprocessing_telemetry = f"Ontology Map Generated. Sanitized {len(df.columns)} columns."

            conn = sqlite3.connect(':memory:')
            df.to_sql('dataset', conn, index=False, if_exists='replace')
            cursor = conn.cursor()
            update_status(table, task_id, "ingesting", "Dataset cleaned and loaded into ephemeral memory.")

            update_status(table, task_id, "planning", "Mapping intent to Ontology...")
            initial_sql = generate_sql(system_state, ontology_map)
            
            update_status(table, task_id, "executing", "Executing dynamic SQL against data matrix...", initial_sql)
            execution_results, final_sql = evaluate_and_fix_sql(cursor, initial_sql, ontology_map, task_id, table)
            
            full_data_sample = {k: v['full'] for k, v in execution_results.items()} 
            ai_data_sample = {k: v['preview'] for k, v in execution_results.items()} 
            conn.close()

            update_status(table, task_id, "synthesizing", "Generating Strategy Brief...")
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            clean_feature_cols = [col for col in numeric_cols if col not in ['patient_id', 'member_id', 'record_index']]
            
            corr_dict = df[clean_feature_cols].corr(numeric_only=True).round(2).to_dict()
            
            raw_json_strategy = synthesize_ontology(ai_data_sample, system_state, clean_feature_cols, corr_dict)
            
            point_analyses = []
            strategy_brief = {}
            try:
                parsed_res = json.loads(raw_json_strategy, strict=False)
                if isinstance(parsed_res, list): parsed_res = parsed_res[0] if parsed_res else {}
                
                strategy_brief = parsed_res.get("strategy_brief", {})
                point_analyses = parsed_res.get("point_analyses", [])
                if isinstance(point_analyses, dict): point_analyses = [point_analyses]
                
                for pt in point_analyses:
                    if isinstance(pt.get("point_answers"), list):
                        pt["point_answers"] = "\n".join(str(x) for x in pt["point_answers"])
                        
            except Exception as e:
                strategy_brief = {"descriptive": "Data analysis complete, narrative failed."}
                point_analyses = [{"point_id": "error", "point_title": "Analysis Processed", "point_answers": "View charts.", "chart_type": "bar"}]
                
            # CHART ENGINE (DYNAMIC DISPATCHER)
            import plotly.express as px
            linked_chart_jsons = []
            for index, point in enumerate(point_analyses):
                c_json = {}
                p_id = point.get("point_id", f"point_{index}")
                c_type = point.get("chart_type", "bar")
                try:
                    if c_type == "heatmap":
                        df_corr = df[clean_feature_cols].corr(numeric_only=True).round(2)
                        fig = px.imshow(df_corr, text_auto='.2f', aspect='auto', color_continuous_scale='RdBu_r')
                    else:
                        query_data = full_data_sample.get(f"Query_{index + 1}", list(full_data_sample.values())[0] if full_data_sample else [])
                        df_chart = pd.DataFrame(query_data)
                        
                        if not df_chart.empty and 'error' in df_chart.columns:
                            raise ValueError(str(df_chart['error'].iloc[0]))

                        x_col, y_col, color_col = point.get("x_col", "x"), point.get("y_col", "y"), point.get("color_col")
                        
                        if df_chart.empty:
                            fig = px.bar(x=["No Data"], y=[0])
                        else:
                            if x_col not in df_chart.columns: x_col = df_chart.columns[0]
                            if y_col not in df_chart.columns: y_col = df_chart.columns[1] if len(df_chart.columns) > 1 else df_chart.columns[0]
                            
                            actual_color = color_col if color_col in df_chart.columns else None
                            
                            # AESTHETIC FALLBACK: If AI is lazy, color by X-axis automatically
                            if not actual_color and c_type in ["box", "violin", "bar"]:
                                actual_color = x_col

                            # THE DYNAMIC DISPATCHER
                            if c_type == "pie":
                                fig = px.pie(df_chart, names=x_col, values=y_col, color=actual_color)
                            else:
                                if hasattr(px, c_type):
                                    plot_func = getattr(px, c_type)
                                    fig = plot_func(df_chart, x=x_col, y=y_col, color=actual_color)
                                else:
                                    # Fallback
                                    fig = px.bar(df_chart, x=x_col, y=y_col, color=actual_color, barmode='group')
                    
                    fig.update_layout(template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(t=20, b=50, l=50, r=20))
                    c_json = json.loads(fig.to_json())
                    c_json["meta"] = {"scenario_id": p_id}
                
                except Exception as chart_error:
                    fig_e = px.bar(x=["Data Extraction Failed"], y=[0], title=f"Error: {str(chart_error)[:60]}...")
                    fig_e.update_layout(template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='red'))
                    c_json = json.loads(fig_e.to_json())
                    c_json["meta"] = {"scenario_id": p_id}
                    
                linked_chart_jsons.append(c_json)

            history_prompt = {"role": "user", "content": str(user_prompt)}
            history_system = {"role": "assistant", "content": "Processed requested data."}
            
            res_history_get = table.get_item(Key={'task_id': task_id})
            current_history_str = res_history_get.get('Item', {}).get('conversation_history', '[]')
            try:
                current_history = json.loads(current_history_str)
            except:
                current_history = []
                
            # PHASE 6: TIMESTAMPED CHAT SYSTEM
            current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            
            history_prompt = {
                "role": "user", 
                "timestamp": current_time,
                "content": str(user_prompt)
            }
            history_system = {
                "role": "assistant", 
                "timestamp": current_time,
                "content": str(strategy_brief.get("descriptive", "Processed data."))
            }
            
            res_history_get = table.get_item(Key={'task_id': task_id})
            current_history_str = res_history_get.get('Item', {}).get('conversation_history', '[]')
            try:
                current_history = json.loads(current_history_str)
            except:
                current_history = []
                
            current_history.extend([history_prompt, history_system])
            
            # Keep the last 10 messages in DynamoDB (5 turns of conversation)
            pruned_history = current_history[-10:]
            
            table.update_item(
                Key={'task_id': task_id},
                UpdateExpression="SET task_status = :s, ai_analysis = :a, chart_data = :c, current_phase = :p, conversation_history = :h, last_updated = :t",
                ExpressionAttributeValues={
                    ':s': 'completed',
                    ':a': json.dumps({"strategy_brief": strategy_brief, "point_analyses": point_analyses, "raw_sql": final_sql, "preprocessing_log": preprocessing_telemetry}),
                    ':c': json.dumps(linked_chart_jsons), 
                    ':p': 'done',
                    ':h': json.dumps(pruned_history),
                    ':t': current_time # Save a master timestamp for the Workspace History UI
                }
            )

        except Exception as e:
            table.update_item(Key={'task_id': task_id}, UpdateExpression="SET task_status = :s, error_msg = :e", ExpressionAttributeValues={':s': 'failed', ':e': str(e)})

    return {'statusCode': 200, 'body': 'Batch Processed'}
