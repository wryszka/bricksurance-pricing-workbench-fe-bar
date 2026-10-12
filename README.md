# Bricksurance Pricing Workbench — governed motor & commercial pricing on Databricks

> **▶ Live app** (open to all Databricks colleagues, Databricks SSO):
> - Via **[go/bricksurance](https://go/bricksurance)**, the Bricksurance estate front door: **Pricing workbench** tile → **gen2 Beta** button. This repo is the gen2 build.
> - Direct link: **https://pricing-workbench-gen2-7474655676955816.aws.databricksapps.com**. The guided Price Optimisation demo is at [`/optimisation-demo`](https://pricing-workbench-gen2-7474655676955816.aws.databricksapps.com/optimisation-demo).
>
> **🎬 Demo recording:** [Bricksurance — Pricing Workbench (YouTube)](https://www.youtube.com/watch?v=p9UTKOY7AKc)
>
> **📊 Deck:** [Actuarial Pricing on Databricks](https://docs.google.com/presentation/d/1dIvzuaXDl_lCNuW153e0pzewNeVCJIF9ynzoWz842k8) (Databricks-internal)
>
> **✅ Proof every layer ran:** [evidence/RUN_EVIDENCE.md](evidence/RUN_EVIDENCE.md) · **🤖 How it was built with AI:** [BUILD.md](BUILD.md)

## Part of Bricksurance — an open-source insurance operating system on Databricks

This workbench is one module of **Bricksurance**: a fictional European insurer ("Bricksurance SE") whose whole operating model — pricing, underwriting, claims, reserving, reinsurance, IFRS 17, Solvency II, life — is built open-source on the Databricks Data Intelligence Platform. Every module shares the same principles: one governed Unity Catalog estate, serverless and scale-to-zero, real Databricks services (no mock AI), human-in-the-loop decisions with an immutable audit trail, and synthetic data so anyone can deploy it.

| Module | Repo |
|---|---|
| **Pricing (this repo)** | `wryszka/bricksurance-pricing-workbench-fe-bar` |
| Estate front door / hub ([go/bricksurance](https://go/bricksurance)) | [actuarial-workbench](https://github.com/wryszka/actuarial-workbench) |
| Common data layer (ACORD-aligned) | [bricksurance-data-core](https://github.com/wryszka/bricksurance-data-core) |
| Underwriting | [underwriting-workbench](https://github.com/wryszka/underwriting-workbench) |
| Claims | [claims_workbench](https://github.com/wryszka/claims_workbench) |
| Reserving | [reserving-workbench](https://github.com/wryszka/reserving-workbench) · [claim-to-confidence](https://github.com/wryszka/claim-to-confidence) |
| Reinsurance | [reinsurance_workbench](https://github.com/wryszka/reinsurance_workbench) |
| IFRS 17 | [ifrs17-workbench](https://github.com/wryszka/ifrs17-workbench) |
| Solvency II | [solvency_workbench](https://github.com/wryszka/solvency_workbench) |
| Life | [lifecast](https://github.com/wryszka/lifecast) |
| Rate indications | [rate-indications-workbench](https://github.com/wryszka/rate-indications-workbench) |

## The customer problem

**Buyer:** Head of Pricing / Chief Underwriting Officer at a UK personal-lines and commercial insurer. **Domain owner:** the pricing actuaries and pricing analysts.

- **Pricing is slow and fragmented.** Data prep, model building, rating-engine hand-off and monitoring run in separate tools, joined by spreadsheets and file transfers. Taking a new rating factor from idea to live price takes months.
- **Regulators want evidence, not assertions.** The FCA's pricing-practices rules (GIPP — no price-walking at renewal) and Consumer Duty fair-value requirements mean every price change needs a traceable answer: which data, which model, who approved it, and was it fair.
- **Optimisation is a black box.** Demand and elasticity models change the price customers see, but few teams can show the corridor, the constraints and the human sign-off behind an optimised price.

## What this build does about it — the outcome

One governed loop on Databricks: **external data → features → models → optimised price → human approval → deployment → monitoring**, all in one Unity Catalog estate with lineage and an immutable audit log.

| Outcome (synthetic book, verified on the live build) | Result |
|---|---|
| Price optimisation uplift, motor new business | **+£1.00m (+10.7%)**: £9.38m hold-price → £10.39m optimised, all inside a ±15% corridor around the technical (break-even) price |
| Closed loop — did the deployed prices do what the model said? | Predicted profit £8.79m vs realised £8.77m (**−0.2%**) one month on |
| Value of real external data | Real UK postcode enrichment (1.5M postcodes) lifts **Gini 0.11 → 0.25**, deviance explained 1.0% → 5.3% |
| Approval control | No price reaches production without a named human approval; the corridor is re-checked **server-side** and the decision is written to an immutable record |

## Six Databricks layers, one connected journey

One governed Unity Catalog estate runs through every layer: external and internal pricing data are ingested and governed, served online, modelled, explained by agents, questioned in Genie, and priced and approved in the app.

| Layer | What it does here | Where to look |
|---|---|---|
| **1. Lakeflow + Unity Catalog** | Volume → bronze ingestion; Lakeflow Declarative Pipeline (silver) with data-quality expectations; gold Unified Pricing Table; 65 Lakeflow Jobs, with one `full_build` job that populates everything; UC tags, column descriptions, lineage, audit log, regulatory export | `resources/ingestion_pipeline.yml`, `src/01_ingestion/`, `src/02_silver/*.sql`, `src/03_gold/`, `resources/full_build.yml`, `src/00_setup/apply_tags.py`, `src/08_governance/` |
| **2. Lakebase** | Online feature store (`motor-pricing-online-store`, `pricing-upt-online-store`) for low-latency feature lookups at quote time | `src/07_serving/setup_online_store.py`, `src/07_serving/test_online_lookup.py`, `src/04_models/production/motor_pricing_scorer.py` |
| **3. ML (deep component)** | Frequency/severity GLMs, demand, fraud and retention GBMs, monotone LightGBM elasticity models; a 50-spec GLM model factory; MLflow tracking, UC registry with `@champion` aliases, governance PDF per model; real-time Model Serving scorers; a constrained price-optimisation solver | `src/04_models/production/`, `resources/factory_train.yml`, `src/07_serving/`, `resources/optimisation.yml` |
| **4. GenAI agents** | Mosaic AI Agent Framework agents on Claude (Foundation Model API): pricing chat agent with personas (rate-change, drift sentinel, constraint author, committee scribe, price explainer…) and a governance agent. Tools read governed UC data; the whole optimisation loop is also exposed as an **MCP** tool surface, with the deploy gate enforced inside the tool | `src/04_models/production/pricing_chat_agent.py`, `src/04_models/production/governance_agent.py`, `src/app/server/routes/mcp.py`, `docs/MCP_INTERFACE_SPEC.md` |
| **5. Genie** | Two curated Genie spaces over the governed gold tables: *Modelling Mart — Pricing Q&A* and *Commercial Quote Review*, embedded in the app | `src/00_setup/create_ai_assets.py`, `src/app/server/routes/genie.py` |
| **6. Databricks App** | FastAPI + React workbench: data ingestion with HITL approval, feature store, model factory, deployment, quote review, monitoring, governance, and the seven-tab Price Optimisation loop (approve → deploy) | `src/app/`, `resources/app.yml` |

**Deployed vs illustrative:** everything above is deployed and has run (see [evidence](evidence/RUN_EVIDENCE.md)). Policy, quote and claim data are **synthetic**; the postcode enrichment uses **real public UK data** (ONSPD, IMD 2019, ONS RUC). No customer data is used anywhere.

---

## The flow, literally

```
External data ─ enrichment ─┐
  (ONSPD + IMD + market +   │
   geo + credit bureau)     ├─→ Quote request ─→ Pricing model ─→ Quote response
                            │     (Jane)          (freq × severity)
                            │         │
                            │         └─ if bound ─→ Policy ─ accrues ─→ Claims
                            │                           │
                            │                           └─→ Training feature store
                            └───────────────────────────────┘        │
                                                                    retrain
```

- **Training feature store** = policy-keyed Delta table, 50K rows with features at policy inception + observed outcomes. What the GLMs and GBMs learn from. Backed by a promotable online store (Lakebase) for sub-10ms lookups at serving time.
- **Quote stream** = the serving-time feature shape. Each quote is captured as three JSON payloads in Unity Catalog — sales request, rating-engine call, rating-engine response. Same rows train the Demand GBM.
- **External data** = joined at both quote and policy time. Includes the real 1.5M English postcode enrichment (ONSPD + IMD 2019 + ONS RUC + coastal flags) so the feature catalog has real lineage, not synthetic stubs.
- **Feature catalog** = one row per feature in the UPT, with source tables, transformation, owner, regulatory/PII flags. Foundation for feature-level lineage and audit bolt-ons.

## What's in the app

- **External Data** — 4 datasets visible, including the real UK postcode enrichment. HITL approval flow for the synthetic ones.
- **Quote Review** — transaction lookup, JSON payload view, simulated replay, Claude-backed AI Analyst (placeholder).
- **Feature Store** — offline Delta + online Lakebase status, promote / pause buttons, **feature catalog** with per-feature provenance.
- **Model Development** — notebook inventory + challenger panel showing Gini lift per real-UK factor.
- **Model Factory** — 50-spec GLM factory, leaderboard, governance PDF per model.
- **Model Deployment** — two scoring paths: new-business (feature vector direct) and renewal (FeatureLookup via online store).
- **Quote Review Analytics + Genie** — broader pattern analysis across the quote stream.
- **Monitoring, Governance** — data freshness, DQ, immutable audit log, regulatory export.
- **Price Optimisation** (motor) — the governed optimisation loop, seven tabs: **Optimiser** (objective front door, efficient frontier, per-segment waterfall, factor table, approve→deploy HITL, and **explain-this-price** for any quote), **Decisions** (immutable decision records + committee-paper drafting), **Demand & red-team** (monotone elasticity curves + endogeneity + parameter-recovery), **Monitoring** (drift, deviation, corridor/GIPP breaches, fair-value evidence, closed-loop advance-month), **Aggregator squeeze** (the reactive agentic second act), **Heavy mode** (ensemble disagreement map + exhaustive stochastic run with measured cost), **How it works** (constraint YAML, deep-links, the MCP tool surface). Agents on the managed framework: constraint-author, drift-sentinel, planner, recommender, price-explainer, committee-scribe, model-risk-narrator. Full loop also drivable from an external **MCP** client (10+ `opt_*` tools; the deploy gate is enforced server-side).

## Notebook track for data scientists / actuaries

`src/new_data_impact/` — six standalone notebooks that answer *"does adding real external data actually make pricing models better?"* Standard vs enriched freq+sev GLMs on a 200K portfolio, Claude review agent, governance PDF. Hero numbers: Gini 0.11 → 0.25, Deviance Explained 1.0% → 5.3%.

## Deploy to a fresh workspace

Everything is serverless and scale-to-zero. Three commands after a one-time
config edit. No notebook-by-notebook running — the **Full Build** orchestrator
chains the whole populate in dependency order and is idempotent.

**Prerequisites:** Databricks CLI authenticated to the target workspace
(`databricks auth login --host <url> --profile <PROFILE>`), a serverless SQL
warehouse, and Unity Catalog. That's it — no classic compute needed.

```bash
# 1. Point the target at your workspace
#    Edit databricks.yml → targets.v2: host + profile, and its variables:
#    catalog_name, schema_name (default pricing_workbench), warehouse_id.

# 2. Create the app first so it mints its service principal, then bind + wire it.
#    (The app SP is granted CAN_MANAGE_RUN on jobs, so the bundle needs its id;
#     and the bundle can't adopt an app it didn't create — hence create+bind.)
databricks apps create pricing-workbench --profile <PROFILE>
#    → copy the printed service_principal_client_id into
#      databricks.yml → targets.v2.variables.app_service_principal_id
databricks bundle deployment bind pricing_workbench pricing-workbench \
    --target v2 --profile <PROFILE> --auto-approve

# 3. Deploy the bundle (all jobs + the app resource)
./deploy.sh v2          # builds frontend, syncs app.yaml, deploys bundle + app

# 4. Populate everything — ONE job, ~30-40 min, all serverless
databricks bundle run full_build --target v2 --profile <PROFILE>
#    data → UPT (incl. real 1.5M-row UK postcode enrichment) → 4 champions →
#    aliases → rating config + release rate-book → inference/bias backfills →
#    shadow-pricing → commercial rating-engine endpoint → governance + chat
#    agents (self-mint their SQL token) → app-SP grants → metadata/tags.
```

Open the app (URL from `databricks apps get pricing-workbench`). The commercial
rating engine prices live (scale-to-zero); the agents answer from real tables.

**Notes**
- The agents self-provision a 90-day PAT for their SQL tools on deploy. If your
  workspace disables PATs, inject `AGENT_TOKEN`/`AGENT_HOST` on the two agent
  endpoints instead.
- **Genie spaces + the Modelling-Mart dashboard** are workspace assets you
  create once and wire via `app.v2.yaml` (`GENIE_SPACE_ID`,
  `GENIE_QUOTE_SPACE_ID`, `MART_DASHBOARD_ID`); until set, those tabs stay
  hidden.
- The optional **live-serving tier** (Lakebase online store + route-optimized
  scorer + QPS load tester) is *not* part of Full Build — it costs money while
  up. Arm it on demand with `databricks bundle run live_pricing_provision` and
  tear it down with `live_pricing_teardown`.

## Two tracks

| Track | For | Entry point |
|---|---|---|
| **Pricing Workbench app** | Execs, underwriters, operators, actuaries | React app — sidebar: Data Ingestion, Model Factory, Quote Review, Governance, etc. |
| **New Data Impact study** (`src/new_data_impact/`) | Data scientists, actuaries, governance | 6 notebooks — build enrichment → train standard vs enriched models → governance PDF → AI agent |

Both tracks share the same Unity Catalog schema (`pricing_workbench_gen2`). The study's derivative tables are prefixed `impact_*` so they group together in Catalog Explorer; the reusable `postcode_enrichment` reference is used by both tracks.

## Architecture

```
External Data → Volume → Bronze → DLT (expectations) → Silver
                                                          ↓
Internal Data (policies, claims, quotes) ───────→ Unified Pricing Table (Gold)
                                                          ↓
              Feature Lookup → Train 6 Models → MLflow → UC Registry
                                                          ↓
              Online Store (Lakebase) → Model Serving → REST API
                                                          ↓
              GOVERNANCE: UC Lineage │ Audit Log │ Time Travel │ DQ Monitoring
```

## Prerequisites

- Databricks workspace with **serverless compute**
- Unity Catalog enabled
- Databricks CLI v0.200+

## Repository Structure

```
├── databricks.yml              # DABs configuration
├── resources/                  # Job and pipeline definitions
├── src/
│   ├── 00_setup/               # Data generation + overview
│   ├── 01_ingestion/           # CSV → Bronze
│   ├── 02_silver/              # DLT expectations + cleansing
│   ├── 03_gold/                # Unified Pricing Table build
│   ├── 04_models/production/   # GLMs, GBMs, elasticity, model factory, optimisation, agents
│   ├── 05_use_cases/           # Shadow pricing, PIT, new dataset, lineage, enriched pricing
│   ├── 07_serving/             # Lakebase online store + model serving endpoints
│   ├── 08_governance/          # Regulatory export
│   ├── new_data_impact/        # Notebook track: does real external data improve pricing?
│   ├── optimisation_demo/      # Driven optimisation demo chapters
│   ├── app/                    # FastAPI + React HITL application (+ MCP server)
│   └── utils/                  # Shared audit + diagram utilities
├── evidence/                   # Proof the build ran (live state, captured via CLI)
├── BUILD.md                    # How this was built with AI
└── docs/
    ├── talk_track.md           # Executive (30 min) + Technical (60 min)
    ├── data_dictionary.md      # Every table and column documented
    └── about_demo.md           # Deployment guide + feature list
```

## Documentation

- **Demo Runbook** — the presenter guide (pre-demo checklist, demo flow, Q&A cheat sheet) is kept internally, not linked from this public repo.
- **[Talk Track](docs/talk_track.md)** — Executive and technical demo scripts
- **[Data Dictionary](docs/data_dictionary.md)** — Complete table and column reference
- **[About This Demo](docs/about_demo.md)** — Deployment guide, features, disclaimer

## Disclaimer

This is a synthetic demonstration. All company names, policy data, and financial
figures are entirely fictional. No real customer data is used.
