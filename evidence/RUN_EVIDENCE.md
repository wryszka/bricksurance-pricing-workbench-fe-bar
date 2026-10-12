# Run evidence — the build demonstrably ran

Captured **2026-10-12** from the live deployment (workspace `fevm-lr-pricing-v2-aws-us`, catalog `lr_pricing_v2_aws_us_catalog`, schema `pricing_workbench_gen2`) with the Databricks CLI. Everything below is the real state of the deployed build, not a mock.

## Databricks App

| App | URL | App state | Compute | Active deployment |
|---|---|---|---|---|
| `pricing-workbench-gen2` | https://pricing-workbench-gen2-7474655676955816.aws.databricksapps.com | RUNNING | ACTIVE | 01f1af6d181e16e69004ed4e7fda6f68 (2026-09-13, SUCCEEDED) |

## Lakeflow Jobs — last run per job (gen2 bundle)

| Job | Last result | Run date |
|---|---|---|
| Apply Metadata (tables, columns, volumes, models) (gen2) | SUCCESS | 2026-08-26 |
| Build — apply uniform UC tags (gen2) | SUCCESS | 2026-08-26 |
| Build — create Genie spaces + mart dashboard (gen2) | SUCCESS | 2026-08-26 |
| Build — ensure supporting tables (agent resources) (gen2) | SUCCESS | 2026-08-26 |
| Build — generate governance packs (all champions) (gen2) | SUCCESS | 2026-08-26 |
| Build — grant app service principal (UC + endpoints) (gen2) | SUCCESS | 2026-08-26 |
| Build — set champion aliases (gen2) | SUCCESS | 2026-08-26 |
| Build Motor UPT (unified_motor_table_live) (gen2) | SUCCESS | 2026-08-26 |
| Build Postcode Enrichment (real UK public data) (gen2) | SUCCESS | 2026-08-26 |
| Build Unified Pricing Table (Gold) (gen2) | SUCCESS | 2026-08-26 |
| Deploy motor pricing scorer (Large endpoint) (gen2) | SUCCESS | 2026-08-26 |
| Ingest External Data → Raw → Silver (gen2) | SUCCESS | 2026-08-26 |
| Motor live serving: provision (Lakebase + endpoint reconcile) (gen2) | SUCCESS | 2026-09-13 |
| Optimisation demo — Chapter 2 check (gen2) | SUCCESS | 2026-09-12 |
| Optimisation demo — Chapter 2 governance setup (gen2) | SUCCESS | 2026-09-13 |
| Optimisation demo — Chapter 2 prepare (gen2) | SUCCESS | 2026-09-12 |
| Optimisation demo — Chapter 2 run (gen2) | SUCCESS | 2026-09-13 |
| Optimisation demo — Chapter 3 run (gen2) | SUCCESS | 2026-09-13 |
| Optimisation demo — run (gen2) | SUCCESS | 2026-09-13 |
| Optimisation demo — scale benchmark (gen2) | SUCCESS | 2026-09-13 |
| Price optimisation — advance month (did it work?) (gen2) | SUCCESS | 2026-08-25 |
| Price optimisation — arm elasticity scorer (gen2) | SUCCESS | 2026-08-25 |
| Price optimisation — constrained solver (gen2) | SUCCESS | 2026-09-12 |
| Price optimisation — decision records (gen2) | SUCCESS | 2026-09-05 |
| Price optimisation — deploy procedure (UC-native gate) (gen2) | SUCCESS | 2026-09-12 |
| Price optimisation — elasticity models (gen2) | SUCCESS | 2026-09-05 |
| Price optimisation — explain-price function (gen2) | SUCCESS | 2026-09-05 |
| Price optimisation — fairness & fair-value evidence (gen2) | SUCCESS | 2026-09-05 |
| Price optimisation — full build (gen2) | SUCCESS | 2026-09-05 |
| Price optimisation — heavy mode (ensemble + stochastic) (gen2) | SUCCESS | 2026-09-05 |
| Price optimisation — monitoring (gen2) | SUCCESS | 2026-09-05 |
| Price optimisation — motor quote/renewal data (gen2) | SUCCESS | 2026-09-05 |
| Price optimisation — read views (UC-native) (gen2) | SUCCESS | 2026-09-05 |
| Price optimisation — renewal solver (GIPP-enforced) (gen2) | SUCCESS | 2026-09-05 |
| Price optimisation — simulation (gen2) | SUCCESS | 2026-09-05 |
| Price optimisation — tear down elasticity scorer (gen2) | SUCCESS | 2026-08-25 |
| Pricing Workbench — Full Build (populate everything) (gen2) | SUCCESS | 2026-08-26 |
| Setup — Motor Insurance dataset (1M policies) (gen2) | SUCCESS | 2026-08-26 |
| Setup — Pricing UPT Demo (gen2) | SUCCESS | 2026-08-26 |
| Train motor champions (freq + sev + demand + fraud) (gen2) | SUCCESS | 2026-08-26 |
| UC1 — Shadow Pricing Impact Analysis (gen2) | SUCCESS | 2026-08-26 |
| v1 — Bias demographics backfill (director_gender + postcode_demographic) (gen2) | SUCCESS | 2026-08-26 |
| v1 — Compare & test models (gen2) | SUCCESS | 2026-09-13 |
| v1 — Deploy governance agent (gen2) | SUCCESS | 2026-08-26 |
| v1 — Deploy pricing chat agent (gen2) | SUCCESS | 2026-08-26 |
| v1 — Deploy unified pricing scorer endpoint (gen2) | SUCCESS | 2026-08-26 |
| v1 — Inference log backfill (score all UPT policies) (gen2) | SUCCESS | 2026-08-26 |
| v1 — Live pricing: load test (sustained QPS against scorer) (gen2) | SUCCESS | 2026-09-23 |
| v1 — Pricing engine releases seed (monthly rate book) (gen2) | SUCCESS | 2026-08-26 |
| v1 — Rating engine config seed (3 versions) (gen2) | SUCCESS | 2026-08-26 |
| v1 — Train production pricing models (Scene A) (gen2) | SUCCESS | 2026-08-26 |

## Model Serving endpoints

| Endpoint | Ready | Config |
|---|---|---|
| `pwg2_chat_agent` | READY | NOT_UPDATING |
| `pwg2_elasticity_scorer` | READY | NOT_UPDATING |
| `pwg2_governance_agent` | READY | NOT_UPDATING |
| `pwg2_motor_scorer` | READY | NOT_UPDATING |
| `pwg2_motor_scorer_direct` | READY | NOT_UPDATING |
| `pwg2_pricing_scorer` | READY | NOT_UPDATING |

## Lakebase (online feature store)

| Instance | State |
|---|---|
| `pricing-upt-online-store` | AVAILABLE |
| `motor-pricing-online-store` | AVAILABLE |

## Genie spaces

| Space | ID |
|---|---|
| Modelling Mart — Pricing Q&A (gen2) | `01f19bc879b6126ba41ab0b144c9bf11` |
| Commercial Quote Review (gen2) | `01f19bc87a211f238fa76d8e596449a6` |

## Unity Catalog — schema `pricing_workbench_gen2`

- **93 securables**: FOREIGN 1, MANAGED 80, MATERIALIZED_VIEW 3, VIEW 9

**Registered models (UC model registry):**

- `conversion_elasticity_motor`
- `demand_gbm`
- `demand_gbm_motor`
- `fraud_gbm`
- `fraud_gbm_motor`
- `freq_glm`
- `freq_glm_motor`
- `governance_agent`
- `pwg2_chat_agent`
- `pwg2_motor_scorer`
- `pwg2_motor_scorer_direct`
- `pwg2_pricing_scorer`
- `retention_elasticity_motor`
- `sev_glm`
- `sev_glm_motor`

## Verified outcomes

- **Price optimisation (motor):** hold-price book £9.38m → optimised £10.39m, **+£1.00m (+10.7%)**, every segment inside the ±15% corridor around the technical (break-even) price. Source: `docs/DECISIONS.md` (re-measured 2026-08-26).
- **Closed loop ("did it work?"):** after rolling the book forward one synthetic month under the deployed prices, predicted profit £8.79m vs realised £8.77m (**−0.2%**). Source: live query of `optimisation_advance_result`, 2026-10-12.
- **New data impact study:** real UK postcode enrichment lifts Gini 0.11 → 0.25 and deviance explained 1.0% → 5.3%. Source: `src/new_data_impact/`.

_Regenerate:_ `databricks jobs list-runs`, `databricks serving-endpoints list`, `databricks database list-database-instances`, `databricks api get /api/2.0/genie/spaces` against the deployment profile.
