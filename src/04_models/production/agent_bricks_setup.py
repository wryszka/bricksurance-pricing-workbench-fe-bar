# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Bricks — Governance Knowledge Assistant + Pricing Supervisor
# MAGIC
# MAGIC Builds the managed agent layer on top of the workbench:
# MAGIC
# MAGIC 1. **Knowledge Assistant: "Bricksurance Model Governance Assistant (gen2)".** RAG over
# MAGIC    - every champion model's **governance pack** PDF (`/Volumes/.../governance_packs/`), and
# MAGIC    - the **pricing governance policy** (`/Volumes/.../governance_docs/`): decision log, standards,
# MAGIC      the versioned constraint policy (corridor, forbidden signals, GIPP), runbook and governance Q&A.
# MAGIC
# MAGIC    Agent Bricks builds and manages the Vector Search index; answers cite the source document.
# MAGIC 2. **Supervisor Agent: "Bricksurance Pricing Supervisor (gen2)".** The front door that routes each
# MAGIC    question to a governed tool:
# MAGIC
# MAGIC    | Tool | Type | Use |
# MAGIC    |---|---|---|
# MAGIC    | `pricing_kpis` | Genie space over the UC **metric views** | Portfolio and optimisation KPIs |
# MAGIC    | `quote_review` | Genie space | Individual quotes and rating-engine payloads |
# MAGIC    | `governance_assistant` | Knowledge Assistant | Model governance and pricing policy, with citations |
# MAGIC    | `explain_price` | UC function | Why one motor quote got its price |
# MAGIC
# MAGIC **Read-only by design.** The write functions (`deploy_factor_set`, `record_constraint_edit`,
# MAGIC chapter-2 approve) are *not* given to the supervisor. A price can only be deployed by a named
# MAGIC human through the app's approval gate.
# MAGIC
# MAGIC Idempotent: resources are matched by display name and updated in place.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_pricing_v2_aws_us_catalog")
dbutils.widgets.text("schema_name", "pricing_workbench_gen2")
dbutils.widgets.text("docs_path", "")   # repo files root; governance docs are copied from here
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
docs_path = dbutils.widgets.get("docs_path")
fq = f"{catalog}.{schema}"

# COMMAND ----------

import shutil, time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.knowledgeassistants import KnowledgeAssistant, KnowledgeSource
from databricks.sdk.service.supervisoragents import SupervisorAgent, Tool

w = WorkspaceClient()

KA_NAME = "Bricksurance Model Governance Assistant (gen2)"
SA_NAME = "Bricksurance Pricing Supervisor (gen2)"
PACKS = f"/Volumes/{catalog}/{schema}/governance_packs/"
DOCS = f"/Volumes/{catalog}/{schema}/governance_docs/"

# COMMAND ----------

# MAGIC %md ## 1. Governance policy documents → UC volume

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {fq}.governance_docs")
if docs_path:
    root = Path(docs_path)
    for rel, target in [("docs/DECISIONS.md", "DECISIONS.md"), ("STANDARDS.md", "STANDARDS.md"),
                        ("docs/optimisation_runbook.md", "optimisation_runbook.md"), ("docs/DEMO_QA.md", "DEMO_QA.md"),
                        ("src/04_models/production/optimisation_constraints/default.yaml", "pricing_constraint_policy.md")]:
        if (root / rel).exists():
            shutil.copy(root / rel, DOCS + target)
            print(f"✓ {rel} → {DOCS}{target}")

# COMMAND ----------

# MAGIC %md ## 2. Knowledge Assistant

# COMMAND ----------

ka = next((k for k in w.knowledge_assistants.list_knowledge_assistants() if k.display_name == KA_NAME), None)
if ka is None:
    ka = w.knowledge_assistants.create_knowledge_assistant(KnowledgeAssistant.from_dict({
        "display_name": KA_NAME,
        "description": ("Answers questions about the pricing models' governance packs: validation results, performance "
                        "metrics, bias and fairness tests, feature lists, approvals and versions for every champion model "
                        "(frequency, severity, demand, fraud), and about the pricing governance policy. Cites the document used."),
    }))
    print(f"✓ created KA {ka.name}")
else:
    print(f"✓ KA exists {ka.name}")

existing = {s.display_name for s in w.knowledge_assistants.list_knowledge_sources(ka.name)}
for display, desc, path in [
    ("Model governance packs",
     "One PDF per champion model version: model card, validation metrics, lift/Gini, bias and fairness tests, feature importance, approval history.",
     PACKS),
    ("Pricing governance policy",
     "Dated decision log, standards, the versioned price-optimisation constraint policy (corridor, forbidden signals, GIPP), the optimiser runbook and the governance Q&A.",
     DOCS),
]:
    if display not in existing:
        w.knowledge_assistants.create_knowledge_source(ka.name, KnowledgeSource.from_dict(
            {"display_name": display, "description": desc, "source_type": "files", "files": {"path": path}}))
        print(f"✓ source {display}")
w.knowledge_assistants.sync_knowledge_sources(ka.name)
print("✓ sync started; KA endpoint:", ka.endpoint_name)

# COMMAND ----------

# MAGIC %md ## 3. Supervisor Agent

# COMMAND ----------

INSTRUCTIONS = """You are the pricing team's assistant at Bricksurance SE, a UK motor and commercial insurer. Route each question to the best tool:
- Portfolio and price-optimisation KPIs (premium, risk cost, loss ratio, frequency, severity, conversion, expected profit hold vs optimised, frontier) -> pricing_kpis (Genie over governed metric views). Prefer the metric views' named measures.
- Individual quotes, rating-engine requests/responses and sales quote payloads -> quote_review (Genie).
- Why a specific motor quote got its price (technical price, loadings, optimisation factor, corridor clamp, decision record) -> explain_price with the quote id.
- Model governance: validation, Gini/AUC, bias and fairness tests, feature lists, approvals, versions, the pricing constraint policy, GIPP, decisions and their rationale -> governance_assistant (cite the document).
Combine tools when a question spans them. Always state numbers with their source. You are read-only: you can recommend a price plan but never deploy one; deployment needs a named human approval in the Pricing Workbench app. Never use gender, marital status or occupation as a pricing argument."""

sa = next((s for s in w.supervisor_agents.list_supervisor_agents() if s.display_name == SA_NAME), None)
if sa is None:
    sa = w.supervisor_agents.create_supervisor_agent(SupervisorAgent.from_dict({
        "display_name": SA_NAME,
        "description": ("Front door for the pricing workbench: routes pricing, portfolio, quote, optimisation and "
                        "model-governance questions to the right governed tool. Read-only."),
        "instructions": INSTRUCTIONS,
    }))
    print(f"✓ created supervisor {sa.name} → endpoint {sa.endpoint_name}")
else:
    print(f"✓ supervisor exists {sa.name} → endpoint {sa.endpoint_name}")

def genie_id(title):
    for sp in w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", []):
        if sp["title"] == title:
            return sp["space_id"]
    raise ValueError(f"Genie space not found: {title}")

tools = {
    "pricing_kpis": {"tool_type": "genie_space",
        "description": "Genie over the governed semantic layer (UC metric views mv_motor_portfolio, mv_commercial_portfolio, mv_quote_conversion, mv_price_optimisation, mv_price_frontier) plus the unified pricing table. Use for any KPI.",
        "genie_space": {"id": genie_id("Modelling Mart — Pricing Q&A (gen2)")}},
    "quote_review": {"tool_type": "genie_space",
        "description": "Genie over commercial quotes and their rating-engine request/response and sales payloads.",
        "genie_space": {"id": genie_id("Commercial Quote Review (gen2)")}},
    "governance_assistant": {"tool_type": "knowledge_assistant",
        "description": "Knowledge Assistant over every champion model governance pack and the pricing governance policy. Cites the source document.",
        "knowledge_assistant": {"knowledge_assistant_id": ka.id}},
    "explain_price": {"tool_type": "uc_function",
        "description": "Decomposes one motor quote by quote id: technical price, loadings, optimisation factor and decision record, corridor clamp.",
        "uc_function": {"name": f"{fq}.explain_price"}},
}
if not spark.catalog.functionExists(f"{fq}.explain_price"):
    tools.pop("explain_price")   # created by the optimisation spine (enable_optimization=true)
have = {t.tool_id for t in w.supervisor_agents.list_tools(sa.name)}
for tid, spec in tools.items():
    if tid not in have:
        w.supervisor_agents.create_tool(sa.name, Tool.from_dict(spec), tid)
        print(f"✓ tool {tid}")
print("✓ supervisor tools:", sorted(have | set(tools)))
