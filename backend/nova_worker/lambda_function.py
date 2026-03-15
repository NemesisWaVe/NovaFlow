import json
import boto3
import sqlite3
import re
import pandas as pd
from datetime import datetime
import asyncio
import base64
import uuid
import struct

dynamodb = boto3.resource('dynamodb')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
s3 = boto3.client('s3')

TABLE_NAME = 'NovaFlow_Tasks'
S3_BUCKET_NAME = 'YOUR S3'

def update_status(table, task_id, phase, log_message, sql=None, execution_log_append=""):
    try:
        response = table.get_item(Key={'task_id': task_id})
        current_logs = response.get('Item', {}).get('execution_log', '')
        if execution_log_append:
            log_message = f"{current_logs}\n{execution_log_append}" if current_logs else execution_log_append
    except Exception as e:
        print(f"Warning: Failed to fetch current logs for {task_id}: {e}")
        pass # Proceed to update state anyway so UI doesn't hang

    UpdateExp = "SET current_phase = :p, execution_log = :l"
    ExpVals = {':p': phase, ':l': log_message}
    if sql:
        UpdateExp += ", raw_sql = :s"
        ExpVals[':s'] = sql
        
    try:
        table.update_item(Key={'task_id': task_id}, UpdateExpression=UpdateExp, ExpressionAttributeValues=ExpVals)
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to update DynamoDB status to {phase}. Error: {e}")

#1 THE NOVA 2 SONIC BIDIRECTIONAL AUDIO ENGINE
def generate_audio_brief(diagnostic_text, prescriptive_text, task_id):
    raw_text = f"Executive Summary. Diagnostic Analysis: {diagnostic_text} Prescriptive Recommendation: {prescriptive_text}"
    text_to_read = raw_text.replace("_", " ") 

    try:
        audio_b64 = asyncio.run(_generate_nova_sonic_audio(text_to_read))
        ext = "wav"
        mime = "audio/wav"
    except ImportError:
        print("aws_sdk_bedrock_runtime not found in Lambda. Falling back to Amazon Polly.")
        audio_b64 = _generate_polly_audio(text_to_read)
        ext = "mp3"
        mime = "audio/mpeg"
    except Exception as e:
        print(f"Audio Generation Failed: {e}")
        return None
        
    if not audio_b64:
        return None

    try:
        audio_bytes = base64.b64decode(audio_b64)
        file_key = f"audio_artifacts/{task_id}.{ext}"
        
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_key,
            Body=audio_bytes,
            ContentType=mime
        )
        
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': file_key},
            ExpiresIn=86400 
        )
        return presigned_url
    except Exception as e:
        print(f"S3 Audio Upload Failed: {e}")
        return None

async def _generate_nova_sonic_audio(text):
    from aws_sdk_bedrock_runtime.client import BedrockRuntimeClient, InvokeModelWithBidirectionalStreamOperationInput
    from aws_sdk_bedrock_runtime.models import InvokeModelWithBidirectionalStreamInputChunk, BidirectionalInputPayloadPart
    from aws_sdk_bedrock_runtime.config import Config, HTTPAuthSchemeResolver, SigV4AuthScheme
    from smithy_aws_core.identity import EnvironmentCredentialsResolver

    region = "us-east-1"
    config = Config(
        endpoint_uri=f"https://bedrock-runtime.{region}.amazonaws.com",
        region=region,
        aws_credentials_identity_resolver=EnvironmentCredentialsResolver(),
        auth_scheme_resolver=HTTPAuthSchemeResolver(),
        auth_schemes={"aws.auth#sigv4": SigV4AuthScheme(service="bedrock")}
    )
    client = BedrockRuntimeClient(config=config)

    stream = await client.invoke_model_with_bidirectional_stream(
        InvokeModelWithBidirectionalStreamOperationInput(model_id="amazon.nova-2-sonic-v1:0")
    )

    prompt_name = str(uuid.uuid4())
    content_name = str(uuid.uuid4())

    async def send_evt(evt):
        chunk = InvokeModelWithBidirectionalStreamInputChunk(
            value=BidirectionalInputPayloadPart(bytes_=json.dumps(evt).encode('utf-8'))
        )
        await stream.input_stream.send(chunk)

    await send_evt({"event": {"sessionStart": {"inferenceConfiguration": {"maxTokens": 2048, "topP": 0.9, "temperature": 0.7}, "turnDetectionConfiguration": {"endpointingSensitivity": "MEDIUM"}}}})
    await send_evt({"event": {"promptStart": {"promptName": prompt_name, "textOutputConfiguration": {"mediaType": "text/plain"}, "audioOutputConfiguration": {"mediaType": "audio/lpcm", "sampleRateHertz": 24000, "sampleSizeBits": 16, "channelCount": 1, "voiceId": "matthew", "encoding": "base64", "audioType": "SPEECH"}}}})
    await send_evt({"event": {"contentStart": {"promptName": prompt_name, "contentName": content_name, "type": "TEXT", "interactive": True, "role": "USER", "textInputConfiguration": {"mediaType": "text/plain"}}}})

    for i in range(0, len(text), 900):
        await send_evt({"event": {"textInput": {"promptName": prompt_name, "contentName": content_name, "content": text[i:i+900]}}})

    await send_evt({"event": {"contentEnd": {"promptName": prompt_name, "contentName": content_name}}})
    await send_evt({"event": {"promptEnd": {"promptName": prompt_name}}})
    await send_evt({"event": {"sessionEnd": {}}})
    await stream.input_stream.close()

    audio_bytes = bytearray()
    while True:
        try:
            output = await stream.await_output()
            result = await output[1].receive()
            if not result.value or not result.value.bytes_:
                break
            data = json.loads(result.value.bytes_.decode('utf-8'))
            if 'event' in data:
                evt = data['event']
                if 'audioOutput' in evt:
                    chunk = base64.b64decode(evt['audioOutput']['content'])
                    audio_bytes.extend(chunk)
                elif 'completionEnd' in evt or 'sessionEnd' in evt:
                    break
        except Exception as e:
            print(f"Sonic Stream output error: {e}")
            break

    num_channels = 1
    sample_rate = 24000
    bits_per_sample = 16
    byte_rate = sample_rate * num_channels * (bits_per_sample // 8)
    block_align = num_channels * (bits_per_sample // 8)

    wav_header = struct.pack('<4sI4s4sIHHIIHH4sI', b'RIFF', len(audio_bytes) + 36, b'WAVE', b'fmt ', 16, 1, num_channels, sample_rate, byte_rate, block_align, bits_per_sample, b'data', len(audio_bytes))
    return base64.b64encode(wav_header + audio_bytes).decode('utf-8')

def _generate_polly_audio(text):
    polly = boto3.client('polly', region_name='us-east-1')
    response = polly.synthesize_speech(Text=text, OutputFormat='mp3', VoiceId='Matthew', Engine='neural')
    return base64.b64encode(response['AudioStream'].read()).decode('utf-8')


#2 THE UNIFIED, BULLETPROOF NOVA ENGINE
def call_nova(prompt, system_prompt, history=None, require_json=False):
    messages = []
    
    if history:
        for msg in history[-4:]:
            content = str(msg.get('content', ''))
            if "> Dataset mounted" in content or "> Analysis complete" in content or "PIPELINE CRASHED" in content:
                continue
                
            role = "user" if msg.get('role', 'user') == 'user' else "assistant"
            
            markdown_fence = '`' * 3 
            clean_content = content.split(markdown_fence)[0].strip()[:1000]
            
            if not clean_content:
                continue
                
            if not messages and role == 'assistant':
                continue
                
            if messages and messages[-1]['role'] == role:
                messages[-1]['content'][0]['text'] += f"\n\n{clean_content}"
            else:
                messages.append({"role": role, "content": [{"text": clean_content}]})

    if messages and messages[-1]['role'] == 'user':
        messages[-1]['content'][0]['text'] += f"\n\nNEW COMMAND: {prompt}"
    else:
        messages.append({"role": "user", "content": [{"text": prompt}]})

    kwargs = {
        "modelId": "amazon.nova-2-lite-v1:0",
        "messages": messages,
        "system": [{"text": system_prompt}],
        "inferenceConfig": {"temperature": 0.2, "topP": 0.9, "maxTokens": 4000}
    }

    if require_json:
        kwargs["toolConfig"] = {
            "tools": [{
                "toolSpec": {
                    "name": "output_strategy",
                    "description": "Output the exact strategy JSON",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "strategy_brief": {
                                    "type": "object",
                                    "properties": {
                                        "diagnostic": {"type": "string"},
                                        "descriptive": {"type": "string"},
                                        "predictive": {"type": "string"},
                                        "prescriptive": {"type": "string"},
                                        "limitations": {"type": "string"}
                                    },
                                    "required": ["diagnostic", "descriptive", "predictive", "prescriptive", "limitations"]
                                },
                                "point_analyses": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "point_id": {"type": "string"},
                                            "point_title": {"type": "string"},
                                            "point_answers": {"type": "string"},
                                            "chart_type": {"type": "string", "enum": ["scatter", "line", "bar", "box", "violin", "histogram", "pie", "heatmap"]},
                                            "x_col": {"type": "string"},
                                            "y_col": {"type": "string"},
                                            "color_col": {"type": "string"}
                                        },
                                        "required": ["point_id", "point_title", "point_answers", "chart_type", "x_col", "y_col"]
                                    }
                                }
                            },
                            "required": ["strategy_brief", "point_analyses"]
                        }
                    }
                }
            }],
            "toolChoice": {"tool": {"name": "output_strategy"}}
        }

    res = bedrock.converse(**kwargs)

    content_blocks = res.get("output", {}).get("message", {}).get("content", [])
    
    if require_json:
        for block in content_blocks:
            if "toolUse" in block and block["toolUse"]["name"] == "output_strategy":
                return block["toolUse"]["input"] 
        return {} 
    else:
        for block in content_blocks:
            if "text" in block:
                return block["text"].strip()
        return ""

#3 DATA PROFILER
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

#4 SQL CLEANER 
def clean_sql(raw_text):
    match = re.search(r'`{3}(?:sql)?\n?(.*?)`{3}', raw_text, re.DOTALL | re.IGNORECASE)
    if match:
        raw_text = match.group(1)
    match = re.search(r'(?i)(SELECT\s+.*)', raw_text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return raw_text.strip()

#5 MEMORY COMPRESSOR
def compress_memory(new_prompt, chat_history):
    if not chat_history or len(chat_history) == 0:
        return new_prompt

    context_str = ""
    for msg in chat_history[-4:]:
        role = msg.get("role", "user").upper()
        content = msg.get("content", "")
        if len(content) > 1500:
            content = content[:1500] + "... [TRUNCATED]"
        context_str += f"{role}: {content}\n"

    rewriter_sys = """You are an AI Context Manager. 
    1. If the User's New Command refers to previous context (e.g., "compare it to X", "what about Y instead"), rewrite it into a fully complete, standalone command.
    2. Otherwise, output the New Command as-is.
    ONLY output the synthesized command string. No markdown."""
    
    rewriter_prompt = f"CHAT HISTORY:\n{context_str}\n\nUSER'S NEW COMMAND: {new_prompt}\n\nREWRITTEN STANDALONE COMMAND:"
    return call_nova(rewriter_prompt, rewriter_sys)

#6 SQL AGENT
def generate_sql(system_state, ontology_map, chat_history=None):
    sql_system = f"""You are an elite Data Engineer writing SQLite queries.
    DATABASE CONTEXT: Table Name is `dataset`.
    ONTOLOGY MAP: {json.dumps(ontology_map, indent=2)}
    
    CRITICAL RULES:
    1. READ THE CHAT HISTORY to understand context.
    2. START DIRECTLY WITH 'SELECT'.
    3. NO MEDIAN(), PERCENTILE_CONT(), STDEV() or VARIANCE(). Use AVG() and MAX()/MIN().
    4. NO WITHIN GROUP or OVER() clauses.
    5. THE PLOTLY RULE: For Box, Violin, and Scatter plots, DO NOT use GROUP BY. Just SELECT the raw columns needed.
    """
    sql_prompt = f"Write 1 to 3 SQLite queries separated by semicolons for this task:\n{system_state}"
    raw = call_nova(sql_prompt, sql_system, history=chat_history)
    return clean_sql(raw)

#7 CRITIC AGENT 
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
        
        critic_sys = f"""You are an expert SQLite Critic. 
        ERROR: {error_encountered}
        FAILED SQL: {current_sql}
        ONTOLOGY: {json.dumps(ontology_map)}

        FIX THE SQL. 
        1. Use 'FROM dataset'.
        2. NO complex math functions (MEDIAN, STDEV, OVER, WITHIN GROUP).
        3. Respond with ONLY the raw SELECT statement.
        """
        raw = call_nova("Rewrite the broken SQL perfectly.", critic_sys)
        current_sql = clean_sql(raw)
    
    return {"Query_1": {"full": [{"error": f"Failed: {error_encountered}"}], "preview": [{"error": "Failed"}]}}, current_sql

#8 STRATEGY SYNTHESIZER
def synthesize_ontology_structured(ai_data_sample, system_state, clean_feature_cols, correlation_matrix):
    num_queries = len(ai_data_sample.keys())
    
    prompt = f"{system_state}\nSQL Results: {json.dumps(ai_data_sample)}\nCorrelation Matrix: {json.dumps(correlation_matrix)}"
    
    system_prompt = f"""You are an elite Data Scientist. Your Executive Strategy Brief must be thorough but concise.
    
    CRITICAL RULES:
    1. Write exactly 2 to 3 highly detailed, quantitative sentences for each field. 
    2. USE NATURAL LANGUAGE: Do not use raw column names like 'waist_circumference_cm'. Write them normally as 'waist circumference'.
    3. THE COPILOT RULE: For the `prescriptive` field, you MUST FIRST provide 2 sentences of actionable recommendations. THEN, as the very last sentence, ask a predictive follow-up question anticipating the user's next move. 
    **CRITICAL ANTI-ROBOT RULE**: DO NOT ask generic questions like "Would you like to forecast Q3?". Your follow-up question MUST be dynamically generated based on the exact metrics, anomalies, or correlations you just analyzed in this specific dataset.
    4. You must generate EXACTLY {num_queries} items in your `point_analyses` array to match the {num_queries} SQL queries executed.
    5. Never repeat the same `chart_type`. Mix them up (scatter, box, violin, bar, heatmap).
    MANDATORY: You must perform all data aggregations, binning, GROUP BY, and CASE WHEN transformations directly in the SQL query. Do not extract raw rows. Return fully processed SQL data.
    """
    
    return call_nova(prompt, system_prompt, require_json=True)

def generate_smart_title(prompt):
    cleaned_prompt = re.sub(r'(?mi)^(To|From|Subject|Executive Memo):.*$', '', prompt).strip()
    
    sys_msg = "You are an AI indexing system. Generate a 4 to 6 word highly professional title. Output ONLY the raw title. No quotes, no trailing dots."
    title = call_nova(cleaned_prompt[:500], sys_msg)
    
    clean_title = title.replace('"', '').replace("'", "").replace("*", "").strip()
    
    if not clean_title:
        return "Advanced Data Analysis"
        
    return clean_title

#MAIN HANDLER 
def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)

    for record in event['Records']:
        body = json.loads(record['body'])
        task_id = body.get('task_id', 'unknown_task')
        user_prompt = body.get('prompt')
        smart_title = generate_smart_title(user_prompt)
        file_key = body.get('file_key', '')
        chat_history = body.get('chat_history', []) 

        # FIX: S3 Path Traversal Check
        if not file_key or '..' in file_key or file_key.startswith('/'):
            error_msg = f"SECURITY ALERT: Invalid file payload detected: {file_key}"
            print(error_msg)
            table.update_item(Key={'task_id': task_id}, UpdateExpression="SET task_status = :s, error_msg = :e", ExpressionAttributeValues={':s': 'failed', ':e': error_msg})
            continue # Skip processing this malicious record

        try:
            update_status(table, task_id, "planning", "Initializing Memory Compressor...")
            system_state = compress_memory(user_prompt, chat_history)

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
            initial_sql = generate_sql(system_state, ontology_map, chat_history)
            
            update_status(table, task_id, "executing", "Executing dynamic SQL against data matrix...", initial_sql)
            execution_results, final_sql = evaluate_and_fix_sql(cursor, initial_sql, ontology_map, task_id, table)
            
            full_data_sample = {k: v['full'] for k, v in execution_results.items()} 
            ai_data_sample = {k: v['preview'] for k, v in execution_results.items()} 
            conn.close()

            update_status(table, task_id, "synthesizing", "Generating Strategy Brief via Constrained Decoding...")
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            clean_feature_cols = [col for col in numeric_cols if col not in ['patient_id', 'member_id', 'record_index']]
            
            corr_dict = df[clean_feature_cols].corr(numeric_only=True).round(2).to_dict()
            
            parsed_res = synthesize_ontology_structured(ai_data_sample, system_state, clean_feature_cols, corr_dict)
            
            strategy_brief = parsed_res.get("strategy_brief", {})
            point_analyses = parsed_res.get("point_analyses", [])
            
            if not point_analyses or not strategy_brief:
                raise ValueError("Model failed to generate structured output.")
                
            #GENERATE THE AWS SONIC AUDIO BRIEF
            update_status(table, task_id, "synthesizing", "Synthesizing Neural Audio via Nova Sonic...")
            audio_url = generate_audio_brief(
                strategy_brief.get("diagnostic", "Data analyzed."), 
                strategy_brief.get("prescriptive", "Awaiting actions."),
                task_id
            )

            #CHART ENGINE
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

                        df_chart = df_chart.head(300)
                        x_col, y_col, color_col = point.get("x_col", "x"), point.get("y_col", "y"), point.get("color_col")
                        
                        if df_chart.empty:
                            fig = px.bar(x=["No Data"], y=[0])
                        else:
                            if x_col not in df_chart.columns: x_col = df_chart.columns[0]
                            if y_col not in df_chart.columns: y_col = df_chart.columns[1] if len(df_chart.columns) > 1 else df_chart.columns[0]
                            
                            actual_color = color_col if color_col in df_chart.columns else None
                            if not actual_color and c_type in ["box", "violin", "bar"]:
                                actual_color = x_col

                            if c_type == "pie":
                                fig = px.pie(df_chart, names=x_col, values=y_col, color=actual_color)
                            else:
                                plot_args = {'x': x_col}
                                if c_type not in ["histogram"]: 
                                    plot_args['y'] = y_col
                                if actual_color:
                                    plot_args['color'] = actual_color

                                if hasattr(px, c_type):
                                    plot_func = getattr(px, c_type)
                                    fig = plot_func(df_chart, **plot_args)
                                else:
                                    fig = px.bar(df_chart, x=x_col, y=y_col, color=actual_color, barmode='group')
                    
                    fig.update_layout(template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(t=20, b=50, l=50, r=20))
                    c_json = json.loads(fig.to_json())
                    c_json["meta"] = {"scenario_id": p_id}
                
                except Exception as chart_error:
                    err_msg = str(chart_error)[:40]
                    fig_e = px.bar(x=[f"Error: {err_msg}"], y=[0], title="Chart Extraction Failed")
                    fig_e.update_layout(template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='red'))
                    c_json = json.loads(fig_e.to_json())
                    c_json["meta"] = {"scenario_id": p_id}
                    
                linked_chart_jsons.append(c_json)

            current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            history_prompt = {"role": "user", "timestamp": current_time, "content": str(user_prompt)}
            history_system = {"role": "assistant", "timestamp": current_time, "content": str(strategy_brief.get("descriptive", "Processed data."))}
            
            res_history_get = table.get_item(Key={'task_id': task_id})
            current_history_str = res_history_get.get('Item', {}).get('conversation_history', '[]')
            try:
                current_history = json.loads(current_history_str)
            except:
                current_history = []
                
            chat_history.extend([history_prompt, history_system])
            pruned_history = chat_history[-10:] # Keep the last 10 messages
            
            # INJECT AUDIO PAYLOAD INTO DYNAMO DB RESPONSE
            table.update_item(
                Key={'task_id': task_id},
                UpdateExpression="SET task_status = :s, ai_analysis = :a, chart_data = :c, current_phase = :p, conversation_history = :h, last_updated = :t, session_title = :st, prompt_snippet = :ps",
                ExpressionAttributeValues={
                    ':s': 'completed',
                    ':a': json.dumps({
                        "strategy_brief": strategy_brief, 
                        "point_analyses": point_analyses, 
                        "raw_sql": final_sql, 
                        "preprocessing_log": preprocessing_telemetry,
                        "audio_url": audio_url 
                    }),
                    ':c': json.dumps(linked_chart_jsons), 
                    ':p': 'done',
                    ':h': json.dumps(pruned_history),
                    ':t': current_time,
                    ':st': smart_title,
                    ':ps': str(user_prompt) 
                }
            )

        except Exception as e:
            print(f"PIPELINE CRASHED: {str(e)}")
            table.update_item(Key={'task_id': task_id}, UpdateExpression="SET task_status = :s, error_msg = :e", ExpressionAttributeValues={':s': 'failed', ':e': str(e)})

    return {'statusCode': 200, 'body': 'Batch Processed'}
