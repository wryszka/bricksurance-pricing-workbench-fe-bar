# Bricksurance Pricing Workbench — governed motor pricing on Databricks

## ▶ Open the deployed app: **[https://pricing-workbench-gen2-7474655676955816.aws.databricksapps.com](https://pricing-workbench-gen2-7474655676955816.aws.databricksapps.com)**

*Pricing Workbench gen2. Open to all Databricks colleagues; sign in with Databricks SSO. Also reachable via [go/bricksurance](https://go/bricksurance) → Pricing workbench tile → **gen2 Beta**.*

## The business outcome

**Customer:** a UK personal-lines **motor insurer**. **Executive sponsor:** Head of Pricing / Chief Underwriting Officer. **Domain owner:** the pricing actuaries and analysts.

**The problem.** UK motor is priced on aggregators, where the cheapest accurate price wins. The market is set by the most analytical pricers (Admiral and Hastings are the usual benchmarks), whose advantage comes from better models, fresher data and faster price changes. An insurer that reprices more slowly, or on fewer rating factors, gets **adversely selected**: it wins the risks that competitors have priced correctly as worse. For most insurers the pricing chain is still split across separate tools: data preparation, modelling, optimisation, the rating engine and monitoring, joined by exports and spreadsheets. On top of that, the FCA's pricing-practices rules (GIPP) and Consumer Duty fair-value rules require evidence behind every price change.

**What one governed pricing platform gives the insurer.** Five reasons to move pricing onto Databricks, each with what this build demonstrates and how to size it:

| # | Value | What this build demonstrates (live) | How to size it for your book (estimate, your inputs) |
|---|---|---|---|
| **1** | **Better calculations**: sharper risk models and demand-based price optimisation | Real external data lifts model **Gini 0.11 → 0.25**. The demand model **recovers the true price sensitivity** built into the test data (slope −6.0 true vs −5.6 recovered). The optimiser finds **+9.2% expected profit** inside a ±15% corridor of the break-even price. *All on synthetic data: this proves the mechanism, not your uplift* | **Each 1 point of loss ratio on a £500m book is £5m a year.** Industry experience of better segmentation plus demand optimisation is typically low single-digit points. A pilot on your data measures your real number |
| **2** | **Much fresher data**: prices that react in days, not quarters | New data lands, passes **14 quality rules**, and reaches the gold pricing table, the models and the live scorer **in one governed pipeline**. The whole estate rebuilds serverless in **~30–40 minutes**, and an approved rate change deploys from the app | **Every month a price lags the market is a month of adverse selection on aggregators.** Size it as: share of quotes mispriced × average mispricing × months of lag |
| **3** | **More features served live at quote time** | **43 features per motor policy (104 for commercial)** served from **Lakebase** by key. Four models (frequency, severity, demand, fraud) price a quote in **~240 ms** from just its `policy_id`, including real postcode, deprivation and telematics signals | Rating factors you can *use* at the point of quote, not just in offline analysis. Each new predictive factor that a factor-table rating engine can't carry is lift you currently leave out |
| **4** | **Full governance**, evidence the FCA can read | Unity Catalog lineage from source to model to price; an immutable audit log; a **named human approval** with the corridor re-checked server-side; a decision record per price change (models, versions, data snapshot, approver, fair-value result); a GIPP-enforced renewal solver. When the fair-value screen fails, the AI recommender advises **REVISE BEFORE DEPLOY**, and the result is written to the decision record | Hours of audit and remediation work per review cycle replaced by one query. Lower regulatory risk on GIPP and Consumer Duty fair value |
| **5** | **Lower licensing cost** from consolidating classic point tools | Data prep, modelling, optimisation, live serving, BI and the app run on **one platform, pay-per-use and serverless**. It can run alongside an existing rating engine during migration | Add up today's separate licences (data prep, modelling, optimisation, rating engine, BI), minus Databricks consumption. Typically a **seven-figure annual line** for a mid-size insurer, phased out tool by tool |

**For the executive sponsor:** a measurable profit lever, faster reaction to the market, lower tool cost, and a fair-value audit trail the board and the FCA can read. **For the domain owner (pricing actuaries):** one workbench for data, models, optimisation and approval, with more features in production and every number traceable to its source.

*Figures in the middle column come from the live build on a synthetic 50,000-policy motor book (plus real public UK postcode data). Figures in the right column are sizing scenarios to be replaced with the insurer's own inputs; they are not results.*

> **Evidence the build ran (text):** [**evidence/EXECUTION_OUTPUT.md**](evidence/EXECUTION_OUTPUT.md) has verbatim query results, metric-view `MEASURE()` output, model metrics, live model scores, and live answers from the Agent Framework agent, Genie, the Knowledge Assistant and the Supervisor Agent. [evidence/RUN_EVIDENCE.md](evidence/RUN_EVIDENCE.md) has job runs, endpoints, Lakebase and Genie state.
>
> **Live app** (open to all Databricks colleagues, SSO): [go/bricksurance](https://go/bricksurance) → **Pricing workbench** tile → **gen2 Beta**, or directly at **https://pricing-workbench-gen2-7474655676955816.aws.databricksapps.com** (guided demo: [`/optimisation-demo`](https://pricing-workbench-gen2-7474655676955816.aws.databricksapps.com/optimisation-demo)).
>
> **Demo recording:** [YouTube](https://www.youtube.com/watch?v=p9UTKOY7AKc) · **Deck:** [DECK.md](DECK.md) · **Built with AI:** [BUILD.md](BUILD.md)

## One connected journey: follow one policy

Every stage reads the previous stage's governed output in the same Unity Catalog schema. Here is `POL-MOTOR-00000001` (real values from [EXECUTION_OUTPUT.md](evidence/EXECUTION_OUTPUT.md)):

1. **Lakeflow, ingest.** Raw policy, claims, telematics and quote data, plus external datasets (market benchmark, geo-hazard, credit bureau, 1.5M real postcodes), land in bronze. A Lakeflow Declarative Pipeline applies 14 data-quality expectations: 500,000 → 465,730 credit-bureau rows survive to silver.
2. **Unity Catalog, govern and define.** Gold `unified_motor_table_live` joins policies, claims history and telematics. UC lineage records each source. Tables carry ownership and PII tags, and every event goes to an immutable `audit_log`. A **semantic layer of five UC metric views** defines each KPI once (risk cost, frequency, severity, conversion, price vs technical, expected profit hold vs optimised). Forbidden pricing signals (gender, marital status, occupation) are deliberately not exposed as dimensions.
3. **Lakebase, serve.** The gold table is synced to Lakebase (`unified_motor_table_live_online`), the online feature store.
4. **ML, make it intelligent.** `pwg2_motor_scorer` receives only `{"policy_id": "POL-MOTOR-00000001"}`, looks up features from Lakebase, and returns frequency 0.111/yr, severity £7,161, demand 0.55, fraud 0.46, a technical premium of **£1,075.85** and a final premium of **£1,430.88**. The price optimiser then sets that policy's segment factor inside the corridor.
5. **GenAI, explain and govern.** An **Agent Bricks Supervisor Agent** is the front door. It routes each question to Genie over the metric views, to an **Agent Bricks Knowledge Assistant** over every model's governance pack and the pricing policy (Vector Search managed by Agent Bricks, answers cite the document), or to the `explain_price` UC function. It is read-only by design and cannot deploy a price. Underneath, specialised Agent Framework agents (Claude) do the deep work. For example, the `recommender` writes the decision pack and, when the fair-value screen fails, recommends **REVISE BEFORE DEPLOY**.
6. **Genie, ask in plain English.** "Expected profit of the optimised plan versus holding prices, by driver age band?" → Genie answers with `MEASURE()` over the `mv_price_optimisation` metric view, so it uses the same governed definition as the app and the agents. The verbatim SQL and result are in the evidence.
7. **App, act.** The pricing team reviews, approves and deploys in the Databricks App. The ±15% corridor is re-checked server-side, and the approval is written to `optimisation_decision_records`.

## The six layers, where to look

Where each stage lives in the code. Every layer reads and writes the **same** Unity Catalog schema (`pricing_workbench_gen2`). There is no side data and no mocks.

| Layer | What it does here | Where to look |
|---|---|---|
| **1. Lakeflow + Unity Catalog** | Volume → bronze ingestion; Lakeflow Declarative Pipeline (silver) with data-quality expectations; gold Unified Pricing Table; 67 Lakeflow Jobs, with one `full_build` job that populates everything; UC tags, column descriptions, lineage, audit log, regulatory export; **semantic layer of five UC metric views** (`mv_motor_portfolio`, `mv_commercial_portfolio`, `mv_quote_conversion`, `mv_price_optimisation`, `mv_price_frontier`) | `resources/ingestion_pipeline.yml`, `src/01_ingestion/`, `src/02_silver/*.sql`, `src/03_gold/`, `src/03_gold/build_metric_views.py`, `resources/metric_views.yml`, `resources/full_build.yml`, `src/00_setup/apply_tags.py`, `src/08_governance/` |
| **2. Lakebase** | Online feature store (`motor-pricing-online-store`, `pricing-upt-online-store`) for low-latency feature lookups at quote time | `src/07_serving/setup_online_store.py`, `src/07_serving/test_online_lookup.py`, `src/04_models/production/motor_pricing_scorer.py` |
| **3. ML (deep component)** | Frequency/severity GLMs, demand, fraud and retention GBMs, monotone LightGBM elasticity models; a 50-spec GLM model factory; MLflow tracking, UC registry with `@champion` aliases, governance PDF per model; real-time Model Serving scorers; a constrained price-optimisation solver | `src/04_models/production/`, `resources/factory_train.yml`, `src/07_serving/`, `resources/optimisation.yml` |
| **4. GenAI agents** | **Agent Bricks**: a **Supervisor Agent** (*Bricksurance Pricing Supervisor*, read-only, with tools Genie ×2, the Knowledge Assistant and the `explain_price` UC function) and a **Knowledge Assistant** (*Model Governance Assistant*) over the governance-pack PDFs and the pricing policy documents, with Vector Search managed by Agent Bricks. Under it, **Mosaic AI Agent Framework** agents on Claude (Foundation Model API): the pricing chat agent with 12 personas (recommender, rate-change, drift sentinel, constraint author, committee scribe, price explainer…) and a governance agent. **Unity AI Gateway** inference tables log every agent request and response to UC. The optimisation loop is also exposed as an **MCP** tool surface, with the deploy gate enforced inside the tool | `src/04_models/production/agent_bricks_setup.py`, `resources/agent_bricks.yml`, `src/04_models/production/pricing_chat_agent.py`, `src/04_models/production/governance_agent.py`, `src/app/server/routes/mcp.py`, `docs/MCP_INTERFACE_SPEC.md` |
| **5. Genie** | Two curated Genie spaces: *Modelling Mart — Pricing Q&A* over the **metric views** plus the gold pricing table, and *Commercial Quote Review*. Both are embedded in the app and are tools of the Supervisor Agent | `src/00_setup/create_ai_assets.py`, `resources/ai_assets/`, `src/app/server/routes/genie.py` |
| **6. Databricks App** | FastAPI + React workbench: data ingestion with HITL approval, feature store, model factory, deployment, quote review, monitoring, governance, and the seven-tab Price Optimisation loop (approve → deploy) | `src/app/`, `resources/app.yml` |

**Deployed vs illustrative:** everything above is deployed and has run (see [evidence](evidence/RUN_EVIDENCE.md)). Policy, quote and claim data are **synthetic**; the postcode enrichment uses **real public UK data** (ONSPD, IMD 2019, ONS RUC). No customer data is used anywhere.

---

## Decisions and trade-offs

| Decision | Why | Trade-off accepted |
|---|---|---|
| **Optimise against the technical (break-even) price, inside a ±15% corridor** | Keeps every optimised price tied to risk cost. Regulators and actuaries can defend it | Less theoretical upside than an unconstrained optimiser |
| **Human approval is mandatory and re-checked server-side** | Agents and the solver can propose, explain and draft, but only a named human can deploy a price. The same gate guards the app and the MCP tool surface | One extra step before any price goes live |
| **Monotone, explainable models (GLMs for risk, monotone LightGBM for elasticity)** | Price must never make a customer *more* likely to buy, and rating factors must be explainable to the FCA | A small loss of raw accuracy versus unconstrained ML |
| **Lakebase online store, not a warehouse query, for live pricing** | Quote-time pricing needs millisecond key lookups, and the scorer resolves features from just the `policy_id` | A second store to keep in sync (managed by synced tables) |
| **Motor only for optimisation; commercial lines ruled out** | Motor proves the whole loop. A second optimiser would duplicate it and confuse the story | Commercial pricing stops at the model and rating stage |
| **Synthetic book + real public enrichment** | Anyone can deploy it, with no customer data. Real postcodes keep the feature lineage genuine | Absolute £ figures are illustrative; ratios and mechanisms are the point |
| **Agent Bricks Supervisor + Knowledge Assistant on top, Agent Framework agents underneath** | The managed Supervisor gives one governed front door and the Knowledge Assistant gives cited answers over governance documents with no RAG code to maintain. Pricing personas that need custom tools stay as Agent Framework agents | Two agent layers to explain. The Supervisor is read-only, so deploying a price stays a human action in the app |
| **Metric views as the semantic layer** | Loss ratio, risk cost, conversion and expected profit are defined once in Unity Catalog, so Genie, the dashboard and the agents cannot disagree, and ratios re-aggregate correctly | Measures must be added to the YAML before anyone can query a new KPI |
| **Expose the loop as MCP tools as well as the UI** | External agents can drive pricing under exactly the same gates as a human in the app | More surface to secure: authorisation is enforced inside each sensitive tool, not only at the transport |

Full dated log with verification notes: [docs/DECISIONS.md](docs/DECISIONS.md).

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
