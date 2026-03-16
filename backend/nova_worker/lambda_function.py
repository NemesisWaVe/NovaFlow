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
S3_BUCKET_NAME = 'novaflow-data-artifacts-2026'


def update_status(table, task_id, phase, log_message, sql=None, execution_log_append=""):
    try:
        response = table.get_item(Key={'task_id': task_id})
        current_logs = response.get('Item', {}).get('execution_log', '')
        if execution_log_append:
            log_message = f"{current_logs}\n{execution_log_append}" if current_logs else execution_log_append
    except Exception as e:
        print(f"Warning: Failed to fetch current logs for {task_id}: {e}")

    update_exp = "SET current_phase = :p, execution_log = :l"
    exp_vals = {':p': phase, ':l': log_message}
    if sql:
        update_exp += ", raw_sql = :s"
        exp_vals[':s'] = sql

    try:
        table.update_item(Key={'task_id': task_id}, UpdateExpression=update_exp, ExpressionAttributeValues=exp_vals)
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to update DynamoDB status to {phase}. Error: {e}")


# --- Audio Engine ---

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
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=file_key, Body=audio_bytes, ContentType=mime)
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

    wav_header = struct.pack(
        '<4sI4s4sIHHIIHH4sI',
        b'RIFF', len(audio_bytes) + 36, b'WAVE', b'fmt ', 16, 1,
        num_channels, sample_rate, byte_rate, block_align, bits_per_sample,
        b'data', len(audio_bytes)
    )
    return base64.b64encode(wav_header + audio_bytes).decode('utf-8')


def _generate_polly_audio(text):
    polly = boto3.client('polly', region_name='us-east-1')
    response = polly.synthesize_speech(Text=text, OutputFormat='mp3', VoiceId='Matthew', Engine='neural')
    return base64.b64encode(response['AudioStream'].read()).decode('utf-8')


# --- Nova LLM Caller ---

def call_nova(prompt, system_prompt, history=None, require_json=False):
    messages = []

    if history:
        for msg in history[-4:]:
            content = str(msg.get('content', ''))
            if "> Dataset mounted" in content or "> Analysis complete" in content or "PIPELINE CRASHED" in content:
                continue

            role = "user" if msg.get('role', 'user') == 'user' else "assistant"
            clean_content = content.split('```')[0].strip()[:1000]

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
        "modelId": "us.amazon.nova-2-lite-v1:0",
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


# --- Data Profiler ---

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


# --- SQL Cleaner ---

def clean_sql(raw_text):
    match = re.search(r'`{3}(?:sql)?\n?(.*?)`{3}', raw_text, re.DOTALL | re.IGNORECASE)
    if match:
        raw_text = match.group(1)
    match = re.search(r'(?i)(SELECT\s+.*)', raw_text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return raw_text.strip()


# --- Memory Compressor ---

def compress_memory(new_prompt, chat_history):
    if not chat_history:
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


# --- SQL Agent ---

def generate_sql(system_state, ontology_map, chat_history=None):
    sql_system = f"""You are an elite Data Engineer writing SQLite queries.
    DATABASE CONTEXT: Table Name is `dataset`.
    ONTOLOGY MAP: {json.dumps(ontology_map, indent=2)}

    CRITICAL RULES:
    1. READ THE CHAT HISTORY to understand context.
    2. START DIRECTLY WITH 'SELECT'.
    3. ALWAYS write exactly 3 queries with STRICTLY DIFFERENT data shapes:

       - Query 1: CONTEXT-AWARE PRIMARY QUERY.

         *** DISTRIBUTION OVERRIDE (highest priority rule) ***
         If the user's request mentions ANY of these words for their primary analysis task:
         "box plot", "boxplot", "violin", "distribution", "compare distributions", "IQR", "spread",
         then Query 1 MUST be RAW DATA — individual rows, NO GROUP BY, NO AVG/COUNT.
         Include the numeric column(s) being compared AND the categorical grouping column.
         Apply any WHERE filters the user specifies.
         Append LIMIT 500.
         Example pattern (use actual column names from the ONTOLOGY MAP — never invent them):
           SELECT category_col, numeric_col_a, numeric_col_b
           FROM dataset WHERE filter_col < threshold LIMIT 500;

         *** DEFAULT (when no distribution chart is requested) ***
         Use GROUP BY with AVG() or COUNT(). Returns few rows (3-30).
         Best rendered as: bar, pie, or line charts.

       - Query 2: PIVOT / CROSS AGGREGATION.
         If the user asks to compare two variables against a metric (e.g., dim_a vs dim_b vs target_metric),
         use GROUP BY on BOTH dimension columns to produce a grid of averages.
         BINNING RULE FOR PIVOT: If either dimension is a continuous numeric, bin it with CASE WHEN
         before GROUP BY so the grid stays readable (3-5 categories per axis maximum).
         Example: GROUP BY dim_a_category, dim_b_category with AVG(target_metric).
         This produces a 2D pivot table. Best rendered as: heatmap.
         If no cross-dimension analysis is needed, write a secondary GROUP BY aggregation.

       - Query 3: RAW DATA SAMPLE — MANDATORY RULES:
         a. DO NOT use GROUP BY or any aggregation function (AVG, COUNT, SUM, etc.).
         b. SELECT the individual row-level columns needed for distribution analysis.
         c. If the user asks for a violin or box plot segmented by a category, include BOTH
            the numeric target column AND the categorical segmentation column(s).
         d. BINNING RULE: If a continuous numeric column is needed as a segmentation category,
            DO NOT pass it raw. Check its min/max in the ONTOLOGY MAP and bin it with CASE WHEN
            into 3 meaningful tiers using thresholds that make sense for that column's actual range.
            Example pattern (adapt column name and thresholds to the real data):
            CASE
                WHEN some_numeric_col < low_threshold  THEN 'Low'
                WHEN some_numeric_col < high_threshold THEN 'Mid'
                ELSE 'High'
            END AS some_numeric_col_tier
            Apply this binning rule to any continuous column used for grouping/coloring in Query 3.
         e. Append LIMIT 300.

    4. NO MEDIAN(), PERCENTILE_CONT(), STDEV() or VARIANCE(). Use AVG() and MAX()/MIN().
    5. ALL queries must use FROM dataset (no schema prefix).
    """
    sql_prompt = f"Write exactly 3 SQLite queries separated by semicolons for this task:\n{system_state}"
    raw = call_nova(sql_prompt, sql_system, history=chat_history)
    return clean_sql(raw)


# --- SQL Critic ---

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

        update_status(
            table, task_id, "executing",
            f"SQL Failed. Triggering Critic... (Attempt {attempt+1}/{max_retries})",
            current_sql, execution_log_append=f"Error: {error_encountered}"
        )

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


# --- Strategy Synthesizer ---

def synthesize_ontology_structured(ai_data_sample, system_state, clean_feature_cols, correlation_matrix, query_manifest):
    prompt = (
        f"{system_state}\n"
        f"SQL Results (preview): {json.dumps(ai_data_sample)}\n"
        f"Correlation Matrix: {json.dumps(correlation_matrix)}\n"
        f"Query Shape Manifest: {json.dumps(query_manifest)}"
    )

    system_prompt = f"""You are an elite Data Scientist generating a structured analysis strategy.

QUERY SHAPE MANIFEST (THIS IS LAW — DO NOT VIOLATE):
{json.dumps(query_manifest, indent=2)}

CRITICAL CHART ROUTING RULES:
1. Each `point_id` MUST match a key from the Query Shape Manifest (e.g., "Query_1", "Query_2", "Query_3").
   The only exception is a correlation heatmap — set point_id to "heatmap" for that special case.
2. `chart_type` MUST come from the `allowed_chart_types` list for that query's `data_shape`. NO EXCEPTIONS.
   - AGGREGATED queries (few rows, grouped data): ONLY "bar", "pie", or "line".
   - RAW_DISTRIBUTION queries (many rows, raw samples): ONLY "scatter", "box", "violin", or "histogram".
   - Correlation heatmap: "heatmap" (point_id must also be "heatmap").
3. `x_col` and `y_col` MUST EXACTLY match column name strings from the `columns` list for that specific query.
   Do not invent or rename columns. Copy them character-for-character.
4. `color_col` is OPTIONAL. Only populate it if a clearly categorical column exists in that query's columns list.
   If there is any doubt, set it to null. A bad color_col is worse than no color_col.
5. ONE CHART PER QUERY — STRICT. Each `point_id` value may appear AT MOST ONCE across the entire
   point_analyses array. Do NOT create two entries with the same point_id (e.g., two charts both
   using "Query_1"). If a query has multiple numeric columns, pick the SINGLE most analytically
   relevant y_col and create one chart only. Duplicate point_ids are a critical violation.

STRATEGY TEXT RULES:
- Write 2-3 quantitative, insight-rich sentences per field in the strategy_brief.
- Use natural language. Do not expose raw underscore_column_names in strategy text.
- Minimum 1 chart, maximum 4 charts. Choose only what adds genuine analytical value.
"""

    return call_nova(prompt, system_prompt, require_json=True)


# --- Smart Title Generator ---

def generate_smart_title(prompt):
    cleaned_prompt = re.sub(r'(?mi)^(To|From|Subject|Executive Memo):.*$', '', prompt).strip()
    sys_msg = "You are an AI indexing system. Generate a 4 to 6 word highly professional title. Output ONLY the raw title. No quotes, no trailing dots."
    title = call_nova(cleaned_prompt[:500], sys_msg)
    clean_title = title.replace('"', '').replace("'", "").replace("*", "").strip()
    return clean_title if clean_title else "Advanced Data Analysis"


# --- Chart Engine ---

DISTRIBUTION_FALLBACK_CHAIN = ["violin", "box", "histogram", "bar"]
AGGREGATION_FALLBACK_CHAIN = ["bar", "line", "pie"]

DARK_TEMPLATE_ARGS = dict(
    template='plotly_dark',
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    margin=dict(t=40, b=50, l=50, r=20),
    font=dict(family="Inter, sans-serif", size=12)
)


def _build_query_manifest(full_data_sample):
    manifest = {}
    agg_keywords = ['avg', 'count', 'sum', 'total', 'max', 'min', 'rate', 'pct', 'mean']

    for q_name, q_rows in full_data_sample.items():
        if not q_rows:
            continue
        row_count = len(q_rows)
        cols = list(q_rows[0].keys()) if q_rows else []
        is_aggregated = row_count <= 50 or any(
            any(kw in col.lower() for kw in agg_keywords) for col in cols
        )
        manifest[q_name] = {
            "row_count": row_count,
            "columns": cols,
            "data_shape": "AGGREGATED" if is_aggregated else "RAW_DISTRIBUTION",
            "allowed_chart_types": (
                ["bar", "pie", "line"] if is_aggregated
                else ["scatter", "box", "violin", "histogram"]
            )
        }

    return manifest


def _resolve_color_args(df_chart, color_col):
    if not color_col or color_col not in df_chart.columns:
        return {}

    col_data = df_chart[color_col]
    if pd.api.types.is_numeric_dtype(col_data) and col_data.nunique() > 15:
        return {"color": color_col, "color_continuous_scale": "Viridis"}

    df_chart[color_col] = col_data.astype(str)
    return {"color": color_col}


def _make_error_figure(title, detail):
    import plotly.graph_objects as go
    fig = go.Figure()
    fig.add_annotation(
        text=f"<b>{title}</b><br><sup>{detail}</sup>",
        xref="paper", yref="paper", x=0.5, y=0.5,
        showarrow=False, font=dict(size=13, color="#aaaaaa"),
        align="center"
    )
    fig.update_layout(**DARK_TEMPLATE_ARGS, title=title)
    return fig


def _try_render_chart(c_type, df_chart, x_col, y_col, color_kwargs, title):
    import plotly.express as px

    try:
        base_args = {"x": x_col, "y": y_col, "title": title, **color_kwargs}

        if c_type == "violin":
            df_chart[y_col] = pd.to_numeric(df_chart[y_col], errors='coerce')
            if df_chart[y_col].isna().all():
                return None
            return px.violin(df_chart, **base_args, box=True, points="outliers")

        elif c_type == "box":
            df_chart[y_col] = pd.to_numeric(df_chart[y_col], errors='coerce')
            if df_chart[y_col].isna().all():
                return None
            return px.box(df_chart, **base_args, notched=False)

        elif c_type == "scatter":
            df_chart[x_col] = pd.to_numeric(df_chart[x_col], errors='coerce')
            df_chart[y_col] = pd.to_numeric(df_chart[y_col], errors='coerce')
            df_chart.dropna(subset=[x_col, y_col], inplace=True)
            if df_chart.empty:
                return None
            return px.scatter(df_chart, **base_args, opacity=0.7, render_mode="webgl")

        elif c_type == "histogram":
            return px.histogram(df_chart, x=x_col, color=color_kwargs.get("color"), title=title, nbins=40)

        elif c_type == "line":
            return px.line(df_chart, **base_args, markers=True)

        elif c_type == "bar":
            return px.bar(df_chart, **base_args)

        elif c_type == "pie":
            df_chart[y_col] = pd.to_numeric(df_chart[y_col], errors='coerce').fillna(0).abs()
            if df_chart[y_col].sum() == 0:
                return None
            return px.pie(df_chart, names=x_col, values=y_col, color=color_kwargs.get("color"), title=title)

        else:
            return px.bar(df_chart, **base_args)

    except Exception as e:
        print(f"Render attempt failed for type='{c_type}': {e}")
        return None


def _try_render_pivot_heatmap(df_chart, title):
    """Detects a 2-col GROUP BY + 1 metric pattern and renders as a px.imshow heatmap."""
    import plotly.express as px

    cols = list(df_chart.columns)
    if len(cols) != 3:
        return None

    col_a, col_b, metric = cols[0], cols[1], cols[2]
    if df_chart[col_a].nunique() > 30 or df_chart[col_b].nunique() > 30:
        return None

    try:
        df_chart[metric] = pd.to_numeric(df_chart[metric], errors='coerce')
        pivot = df_chart.pivot_table(index=col_b, columns=col_a, values=metric, aggfunc='mean')
        pivot = pivot.round(2).fillna(0)
        fig = px.imshow(
            pivot,
            text_auto='.1f',
            aspect='auto',
            color_continuous_scale='RdYlGn_r',
            title=title,
            labels={"x": col_a, "y": col_b, "color": metric}
        )
        return fig
    except Exception as e:
        print(f"Pivot heatmap attempt failed: {e}")
        return None


def render_chart_with_fallback(point, full_data_sample, df, clean_feature_cols):
    import plotly.express as px

    p_id = point.get("point_id", "Query_1")
    c_type = str(point.get("chart_type", "bar")).lower().strip()
    title = point.get("point_title", "Analysis")
    req_x = str(point.get("x_col", "")).strip()
    req_y = str(point.get("y_col", "")).strip()
    req_c = str(point.get("color_col") or "").strip()

    def _finalise(fig, used_type):
        fig.update_layout(**DARK_TEMPLATE_ARGS)
        c_json = json.loads(fig.to_json())
        c_json["meta"] = {"scenario_id": p_id, "rendered_as": used_type}
        return c_json

    # Correlation heatmap uses the full dataframe, not SQL results
    if c_type == "heatmap" or p_id == "heatmap":
        try:
            num_df = df[clean_feature_cols].select_dtypes(include='number')
            df_corr = num_df.corr().fillna(0).round(2)
            fig = px.imshow(df_corr, text_auto='.2f', aspect='auto', color_continuous_scale='RdBu_r', title=title)
            return _finalise(fig, "heatmap")
        except Exception as e:
            return _finalise(_make_error_figure("Heatmap Error", str(e)), "error")

    # Auto-promote to pivot heatmap if the data is a small 2D grid (e.g. stress x sleep)
    if c_type in ("scatter", "line", "bar"):
        probe_data = full_data_sample.get(p_id)
        if probe_data:
            df_probe = pd.DataFrame(probe_data).head(300)
            if len(df_probe.columns) == 3:
                col_a, col_b = df_probe.columns[0], df_probe.columns[1]
                if df_probe[col_a].nunique() <= 8 and df_probe[col_b].nunique() <= 8:
                    fig = _try_render_pivot_heatmap(df_probe.copy(), title)
                    if fig is not None:
                        print(f"Chart Engine: auto-promoted '{c_type}' -> pivot_heatmap for {p_id}")
                        return _finalise(fig, "pivot_heatmap")

    query_data = full_data_sample.get(p_id)
    if not query_data:
        query_data = next((v for v in full_data_sample.values() if v), None)
        print(f"Warning: No data for point_id='{p_id}'. Using fallback query data.")

    if not query_data:
        return _finalise(_make_error_figure("No Data Available", f"All queries returned empty results for {p_id}"), "error")

    df_chart = pd.DataFrame(query_data)
    if df_chart.empty or "error" in df_chart.columns:
        return _finalise(_make_error_figure("Query Error", f"{p_id} returned an error result"), "error")

    df_chart = df_chart.head(300).copy()
    cols = list(df_chart.columns)

    x_col = req_x if req_x in cols else cols[0]
    y_col = req_y if req_y in cols else (cols[1] if len(cols) > 1 else cols[0])

    if x_col != req_x:
        print(f"Warning: x_col '{req_x}' not found in {cols}. Using '{x_col}'.")
    if y_col != req_y:
        print(f"Warning: y_col '{req_y}' not found in {cols}. Using '{y_col}'.")

    # Auto-bin continuous numeric color columns for violin/box to avoid unreadable rainbow legends
    if c_type in {"violin", "box"} and req_c and req_c in cols:
        color_series = df_chart[req_c]
        if pd.api.types.is_numeric_dtype(color_series) and color_series.nunique() > 8:
            try:
                bin_col_name = f"{req_c}_tier"
                low_thresh = round(float(color_series.quantile(0.33)), 1)
                high_thresh = round(float(color_series.quantile(0.67)), 1)
                df_chart[bin_col_name] = pd.cut(
                    color_series,
                    bins=[-float('inf'), low_thresh, high_thresh, float('inf')],
                    labels=[f"Low (<{low_thresh})", f"Mid ({low_thresh}-{high_thresh})", f"High (>{high_thresh})"]
                ).astype(str)
                req_c = bin_col_name
                cols = list(df_chart.columns)
                print(f"Chart Engine: auto-binned '{color_series.name}' -> '{bin_col_name}' (thresholds: {low_thresh}, {high_thresh})")
            except Exception as e:
                print(f"Chart Engine: auto-binning failed for '{req_c}': {e}")
                req_c = ""

    color_kwargs = _resolve_color_args(df_chart, req_c if req_c in cols else None)

    if c_type in DISTRIBUTION_FALLBACK_CHAIN:
        chain = DISTRIBUTION_FALLBACK_CHAIN[DISTRIBUTION_FALLBACK_CHAIN.index(c_type):]
    elif c_type in AGGREGATION_FALLBACK_CHAIN:
        chain = AGGREGATION_FALLBACK_CHAIN[AGGREGATION_FALLBACK_CHAIN.index(c_type):]
    else:
        chain = [c_type, "bar"]

    fig = None
    used_type = c_type
    for attempt_type in chain:
        print(f"Chart Engine: attempting '{attempt_type}' for {p_id}...")
        fig = _try_render_chart(attempt_type, df_chart.copy(), x_col, y_col, color_kwargs, title)
        if fig is not None:
            used_type = attempt_type
            break

    if fig is None:
        fig = _make_error_figure("Render Failed", f"All fallbacks exhausted for chart_type='{c_type}' on {p_id}")

    if used_type != c_type:
        print(f"Chart Engine: {c_type} -> {used_type} for {p_id} (lateral fallback applied)")

    return _finalise(fig, used_type)


# --- Lambda Handler ---

def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)

    for record in event['Records']:
        body = json.loads(record['body'])
        task_id = body.get('task_id', 'unknown_task')
        user_prompt = body.get('prompt')
        smart_title = generate_smart_title(user_prompt)
        file_key = body.get('file_key', '')
        chat_history = body.get('chat_history', [])

        # Block path traversal attempts
        if not file_key or '..' in file_key or file_key.startswith('/'):
            error_msg = f"SECURITY ALERT: Invalid file payload detected: {file_key}"
            print(error_msg)
            table.update_item(
                Key={'task_id': task_id},
                UpdateExpression="SET task_status = :s, error_msg = :e",
                ExpressionAttributeValues={':s': 'failed', ':e': error_msg}
            )
            continue

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

            query_manifest = _build_query_manifest(full_data_sample)

            update_status(table, task_id, "synthesizing", "Generating Strategy Brief via Constrained Decoding...")
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            # Exclude surrogate ID columns — any numeric column whose name suggests it's
            # a row identifier rather than a meaningful feature (id, index, key, code, num, no)
            id_patterns = re.compile(r'\b(id|index|key|code|num|no|number|seq|sequence|row)\b', re.IGNORECASE)
            clean_feature_cols = [col for col in numeric_cols if not id_patterns.search(col)]
            corr_dict = df[clean_feature_cols].corr(numeric_only=True).round(2).to_dict()

            parsed_res = synthesize_ontology_structured(
                ai_data_sample, system_state, clean_feature_cols, corr_dict, query_manifest
            )

            strategy_brief = parsed_res.get("strategy_brief", {})
            point_analyses = parsed_res.get("point_analyses", [])

            # Deduplicate by point_id — LLM occasionally maps two charts to the same query
            seen_ids = set()
            deduped = []
            for p in point_analyses:
                pid = p.get("point_id")
                if pid not in seen_ids:
                    seen_ids.add(pid)
                    deduped.append(p)
                else:
                    print(f"Dedup: dropped duplicate point_id='{pid}'.")
            point_analyses = deduped

            if not point_analyses or not strategy_brief:
                raise ValueError("Model failed to generate structured output.")

            update_status(table, task_id, "synthesizing", "Synthesizing Neural Audio via Nova Sonic...")
            audio_url = generate_audio_brief(
                strategy_brief.get("diagnostic", "Data analyzed."),
                strategy_brief.get("prescriptive", "Awaiting actions."),
                task_id
            )

            linked_chart_jsons = []
            for point in point_analyses:
                c_json = render_chart_with_fallback(point, full_data_sample, df, clean_feature_cols)
                linked_chart_jsons.append(c_json)

            current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            history_prompt = {"role": "user", "timestamp": current_time, "content": str(user_prompt)}
            history_system = {"role": "assistant", "timestamp": current_time, "content": str(strategy_brief.get("descriptive", "Processed data."))}

            res_history_get = table.get_item(Key={'task_id': task_id})
            current_history_str = res_history_get.get('Item', {}).get('conversation_history', '[]')
            try:
                current_history = json.loads(current_history_str)
            except Exception:
                current_history = []

            chat_history.extend([history_prompt, history_system])
            pruned_history = chat_history[-10:]

            table.update_item(
                Key={'task_id': task_id},
                UpdateExpression=(
                    "SET task_status = :s, ai_analysis = :a, chart_data = :c, "
                    "current_phase = :p, conversation_history = :h, last_updated = :t, "
                    "session_title = :st, prompt_snippet = :ps"
                ),
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
            table.update_item(
                Key={'task_id': task_id},
                UpdateExpression="SET task_status = :s, error_msg = :e",
                ExpressionAttributeValues={':s': 'failed', ':e': str(e)}
            )

    return {'statusCode': 200, 'body': 'Batch Processed'}
