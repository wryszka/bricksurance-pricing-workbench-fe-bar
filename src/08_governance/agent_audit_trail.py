# Databricks notebook source
# MAGIC %md
# MAGIC # Agent audit trail — every AI explanation and recommendation, next to the price it reviewed
# MAGIC
# MAGIC One governed, continuously queryable record of what every agent said, in Unity Catalog:
# MAGIC
# MAGIC | Agent layer | Agents | Where each interaction is captured |
# MAGIC |---|---|---|
# MAGIC | **Agent Bricks** | Pricing Supervisor, Model Governance Assistant (Knowledge Assistant) | MLflow traces on each agent's experiment → synced here into `agent_trace_log` |
# MAGIC | **Agent Framework** | `pwg2_chat_agent` (12 personas), `pwg2_governance_agent` | Unity AI Gateway inference tables (`*_payload`) |
# MAGIC
# MAGIC `v_agent_audit_trail` unions both and links each interaction to what it reviewed:
# MAGIC - the **motor quote** it explained (`MQ-…` → technical and offered premium from `optimisation_quote_response`),
# MAGIC - the **governance pack** it discussed (`GP-…`),
# MAGIC - the **price plan in force** when it answered (latest `optimisation_solve` in `audit_log` at that time:
# MAGIC   objective, expected profit uplift, corridor check).
# MAGIC
# MAGIC `mv_agent_audit` puts the trail in the semantic layer (interactions, quote-level
# MAGIC explanations, latency, error rate by agent and day), so Genie and dashboards read the
# MAGIC same definitions. Runs on a schedule; each run is incremental.

# COMMAND ----------

# MAGIC %pip install "mlflow[databricks]>=3.1" "databricks-sdk>=0.152" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_pricing_v2_aws_us_catalog")
dbutils.widgets.text("schema_name", "pricing_workbench_gen2")
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
fqn = f"{catalog}.{schema}"

KA_NAME = "Bricksurance Model Governance Assistant (gen2)"
SA_NAME = "Bricksurance Pricing Supervisor (gen2)"

# COMMAND ----------

import json
from datetime import datetime, timezone
import mlflow
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
agents = []
for ka in w.knowledge_assistants.list_knowledge_assistants():
    if ka.display_name == KA_NAME:
        agents.append((ka.display_name, ka.endpoint_name, ka.experiment_id))
for sa in w.supervisor_agents.list_supervisor_agents():
    if sa.display_name == SA_NAME:
        agents.append((sa.display_name, sa.endpoint_name, sa.experiment_id))
print("Agent Bricks agents:", agents)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fqn}.agent_trace_log (
  trace_id STRING, agent STRING, endpoint_name STRING, request_time TIMESTAMP, duration_ms BIGINT,
  status STRING, question STRING, answer STRING, tools_called ARRAY<STRING>, requester STRING, synced_at TIMESTAMP)
COMMENT 'Every Agent Bricks interaction (Supervisor + Knowledge Assistant), synced from the agents MLflow traces'
""")

# COMMAND ----------

# MAGIC %md ## 1 · Sync new Agent Bricks traces

# COMMAND ----------

def _as_obj(x):
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return x
    return x


def _question(req):
    req = _as_obj(req)
    if isinstance(req, dict) and "request" in req and isinstance(req["request"], (dict, str)):
        req = _as_obj(req["request"])
    items = req.get("input") if isinstance(req, dict) else None
    if isinstance(items, list):
        for it in reversed(items):
            if isinstance(it, dict) and it.get("role") == "user":
                c = it.get("content")
                if isinstance(c, list):
                    return " ".join(p.get("text", "") for p in c if isinstance(p, dict))
                return str(c)
    return str(req)[:4000]


def _answer_and_tools(resp):
    resp = _as_obj(resp)
    tools = []
    if isinstance(resp, dict):
        out = resp.get("output") or []
        texts = []
        for o in out if isinstance(out, list) else []:
            if not isinstance(o, dict):
                continue
            if o.get("type") == "function_call" and o.get("name"):
                tools.append(o["name"])
            if o.get("type") == "message":
                for c in o.get("content") or []:
                    if isinstance(c, dict) and c.get("type") == "output_text":
                        texts.append(c.get("text", ""))
        if resp.get("final_response"):
            return str(resp["final_response"]), tools
        if texts:
            return "\n\n".join(texts), tools
    return str(resp)[:4000], tools


def _span_tools(spans):
    names = []
    for s in spans or []:
        s = s if isinstance(s, dict) else getattr(s, "to_dict", lambda: {})()
        stype = s.get("span_type") or (s.get("attributes") or {}).get("mlflow.spanType", "")
        if str(stype).strip('"').upper() in ("TOOL", "RETRIEVER") and s.get("name"):
            names.append(s["name"])
    return names


last = spark.sql(f"SELECT max(request_time) AS t FROM {fqn}.agent_trace_log").collect()[0]["t"]
since_ms = int(last.replace(tzinfo=timezone.utc).timestamp() * 1000) if last else 0
now = datetime.now(timezone.utc).replace(tzinfo=None)
rows = []
for agent, endpoint, exp_id in agents:
    try:
        df = mlflow.search_traces(experiment_ids=[exp_id], filter_string=f"timestamp_ms > {since_ms}", max_results=5000)
    except Exception:
        df = mlflow.search_traces(experiment_ids=[exp_id], max_results=5000)
        df = df[df["request_time"].astype("int64") > since_ms]
    for _, t in df.iterrows():
        answer, tools = _answer_and_tools(t.get("response"))
        tools = sorted(set(tools + _span_tools(t.get("spans"))))
        meta = t.get("trace_metadata") or {}
        rt = t.get("request_time")
        rt = datetime.fromtimestamp(int(rt) / 1000, tz=timezone.utc).replace(tzinfo=None) if rt is not None else None
        dur = t.get("execution_duration")
        rows.append((t["trace_id"], agent, endpoint, rt, int(dur) if dur is not None else None, str(t.get("state")),
                     _question(t.get("request"))[:8000], answer[:16000], tools,
                     meta.get("mlflow.user") or meta.get("mlflow.trace.user"), now))
print(f"{len(rows)} new Agent Bricks interactions since {last}")

if rows:
    new = spark.createDataFrame(rows, "trace_id STRING, agent STRING, endpoint_name STRING, request_time TIMESTAMP, "
                                      "duration_ms BIGINT, status STRING, question STRING, answer STRING, "
                                      "tools_called ARRAY<STRING>, requester STRING, synced_at TIMESTAMP")
    new.createOrReplaceTempView("new_traces")
    spark.sql(f"""
        MERGE INTO {fqn}.agent_trace_log t USING new_traces s ON t.trace_id = s.trace_id
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md ## 2 · One audit trail across both agent layers, linked to the price reviewed

# COMMAND ----------

def _framework(payload_table, endpoint):
    return f"""
    SELECT databricks_request_id AS interaction_id,
           concat('{endpoint}', coalesce(concat(' · ', get_json_object(request, '$.custom_inputs.persona')), '')) AS agent,
           'Agent Framework' AS agent_layer, '{endpoint}' AS endpoint_name, request_time,
           execution_duration_ms AS duration_ms, CAST(status_code AS STRING) AS status,
           element_at(filter(from_json(get_json_object(request, '$.messages'), 'array<struct<role:string,content:string>>'),
                             m -> m.role = 'user'), -1).content AS question,
           coalesce(element_at(filter(from_json(get_json_object(response, '$.messages'), 'array<struct<role:string,content:string>>'),
                                      m -> m.role = 'assistant'), -1).content,
                    get_json_object(response, '$.choices[0].message.content')) AS answer,
           CAST(NULL AS ARRAY<STRING>) AS tools_called, requester,
           get_json_object(request, '$.custom_inputs.pack_id') AS input_pack_id
    FROM {fqn}.{payload_table}"""

spark.sql(f"""
CREATE OR REPLACE VIEW {fqn}.v_agent_audit_trail
COMMENT 'Every agent interaction (Agent Bricks traces + Agent Framework inference tables), linked to the quote, governance pack and price plan it reviewed'
AS
WITH interactions AS (
  SELECT trace_id AS interaction_id, agent, 'Agent Bricks' AS agent_layer, endpoint_name, request_time, duration_ms,
         status, question, answer, tools_called, requester, CAST(NULL AS STRING) AS input_pack_id
  FROM {fqn}.agent_trace_log
  UNION ALL {_framework('pwg2_chat_agent_payload', 'pwg2_chat_agent')}
  UNION ALL {_framework('governance_agent_payload', 'pwg2_governance_agent')}
),
linked AS (
  SELECT i.*,
         nullif(regexp_extract(concat_ws(' ', question, answer), '(MQ-[0-9]{{8}})', 1), '') AS reviewed_quote_id,
         coalesce(input_pack_id,
                  nullif(regexp_extract(concat_ws(' ', question, answer), '(GP-[0-9]+-[a-z_]+-v[0-9]+)', 1), '')) AS reviewed_pack_id
  FROM interactions i
),
plan_asof AS (
  SELECT l.interaction_id,
         max_by(named_struct('solved_at', s.timestamp,
                             'objective', get_json_object(s.details, '$.objective'),
                             'profit_uplift', CAST(get_json_object(s.details, '$.total_uplift') AS DOUBLE),
                             'all_within_corridor', get_json_object(s.details, '$.all_within_corridor')),
                s.timestamp) AS plan
  FROM linked l JOIN {fqn}.audit_log s
    ON s.event_type = 'optimisation_solve' AND s.timestamp <= l.request_time
  GROUP BY l.interaction_id
)
SELECT l.interaction_id, l.agent, l.agent_layer, l.endpoint_name, l.request_time, l.duration_ms, l.status,
       l.question, l.answer, l.tools_called, l.requester,
       l.reviewed_quote_id, q.technical_premium AS reviewed_quote_technical_premium,
       q.offered_premium AS reviewed_quote_offered_premium,
       l.reviewed_pack_id,
       p.plan.solved_at AS plan_in_force_solved_at, p.plan.objective AS plan_in_force_objective,
       p.plan.profit_uplift AS plan_in_force_profit_uplift, p.plan.all_within_corridor AS plan_in_force_within_corridor
FROM linked l
LEFT JOIN {fqn}.optimisation_quote_response q ON q.quote_id = l.reviewed_quote_id
LEFT JOIN plan_asof p ON p.interaction_id = l.interaction_id
""")

# COMMAND ----------

# MAGIC %md ## 3 · The trail in the semantic layer

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fqn}.mv_agent_audit
WITH METRICS LANGUAGE YAML
COMMENT 'Agent governance KPIs over v_agent_audit_trail'
AS $$
version: 1.1
source: {fqn}.v_agent_audit_trail
dimensions:
  - name: Agent
    expr: agent
  - name: Agent Layer
    expr: agent_layer
  - name: Interaction Date
    expr: DATE(request_time)
measures:
  - name: Interactions
    expr: COUNT(1)
  - name: Quote-Level Explanations
    expr: COUNT_IF(reviewed_quote_id IS NOT NULL)
  - name: Governance Pack Reviews
    expr: COUNT_IF(reviewed_pack_id IS NOT NULL)
  - name: Average Latency Seconds
    expr: AVG(duration_ms) / 1000.0
  - name: Error Rate
    expr: AVG(CASE WHEN status IN ('200', 'OK', 'TraceState.OK') THEN 0.0 ELSE 1.0 END)
$$
""")
TAGS = "('pw_project'='pricing_workbench', 'pw_tier'='governance', 'pw_managed_by'='dab', 'pw_contains_pii'='false')"
spark.sql(f"ALTER TABLE {fqn}.agent_trace_log SET TAGS {TAGS}")
for v in ["v_agent_audit_trail", "mv_agent_audit"]:
    spark.sql(f"ALTER VIEW {fqn}.{v} SET TAGS {TAGS}")

# COMMAND ----------

summary = spark.sql(f"""
  SELECT `Agent Layer`, `Agent`, MEASURE(`Interactions`) AS interactions,
         MEASURE(`Quote-Level Explanations`) AS quote_explanations,
         MEASURE(`Governance Pack Reviews`) AS pack_reviews,
         ROUND(MEASURE(`Average Latency Seconds`), 1) AS avg_latency_s
  FROM {fqn}.mv_agent_audit GROUP BY ALL ORDER BY interactions DESC""")
display(summary)

try:
    user = spark.sql("SELECT current_user()").collect()[0][0]
    det = json.dumps({"new_agent_bricks_interactions": len(rows),
                      "total_interactions": spark.table(f"{fqn}.v_agent_audit_trail").count()})
    spark.sql(f"""
        INSERT INTO {fqn}.audit_log
          (event_id, event_type, entity_type, entity_id, entity_version, user_id, timestamp, details, source)
        SELECT uuid(), 'agent_audit_sync', 'agent', 'all_agents', NULL, '{user}', current_timestamp(), '{det}', 'notebook'
    """)
except Exception as e:
    print(f"audit: {e}")
