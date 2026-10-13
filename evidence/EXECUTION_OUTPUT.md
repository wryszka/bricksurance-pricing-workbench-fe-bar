# Execution output — real results from the deployed build (text)

Captured **2026-10-13 19:21 UTC** by querying the live deployment (workspace `fevm-lr-pricing-v2-aws-us`, schema `lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2`) with the Databricks SDK: SQL warehouse statements, Lakeflow pipeline event log, MLflow, Model Serving, the Agent endpoint and the Genie Conversation API. Every block below is the verbatim response, not a mock. Stages follow the journey: Lakeflow → Unity Catalog → Lakebase → ML (with model evidence and drift retraining) → GenAI agents (with LLM-judge evaluation) → Genie → governance audit trail → App.

## 1. Lakeflow — raw ingestion → silver (Declarative Pipeline) → gold

**Row counts at each layer**

```sql
SELECT 'raw_market_pricing_benchmark' AS tbl, count(*) AS row_count FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.raw_market_pricing_benchmark
UNION ALL SELECT 'raw_geospatial_hazard_enrichment', count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.raw_geospatial_hazard_enrichment
UNION ALL SELECT 'raw_credit_bureau_summary', count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.raw_credit_bureau_summary
UNION ALL SELECT 'silver_market_pricing_benchmark', count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.silver_market_pricing_benchmark
UNION ALL SELECT 'silver_geospatial_hazard_enrichment', count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.silver_geospatial_hazard_enrichment
UNION ALL SELECT 'silver_credit_bureau_summary', count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.silver_credit_bureau_summary
UNION ALL SELECT 'postcode_enrichment (real ONSPD/IMD)', count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.postcode_enrichment
UNION ALL SELECT 'motor_policies', count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.motor_policies
UNION ALL SELECT 'motor_claims_history', count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.motor_claims_history
UNION ALL SELECT 'quotes', count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.quotes
UNION ALL SELECT 'unified_pricing_table_live (gold)', count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live
UNION ALL SELECT 'unified_motor_table_live (gold)', count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_motor_table_live
```
| tbl | row_count |
|---|---|
| silver_market_pricing_benchmark | 141 |
| silver_geospatial_hazard_enrichment | 258 |
| silver_credit_bureau_summary | 465730 |
| raw_market_pricing_benchmark | 150 |
| raw_geospatial_hazard_enrichment | 295 |
| raw_credit_bureau_summary | 500000 |
| postcode_enrichment (real ONSPD/IMD) | 1492543 |
| motor_policies | 50000 |
| motor_claims_history | 25272 |
| quotes | 1200000 |
| unified_pricing_table_live (gold) | 500000 |
| unified_motor_table_live (gold) | 50000 |

**Pipeline** `Silver — External Data Cleansing (gen2)` (id `a29452b1-abfa-4932-8176-e15edbfbe227`), state `IDLE`.
- update `18fbf54a-50ad-4ee9-9377-99d1ef668f15` — COMPLETED — created 2026-08-26 10:39
- update `65d3a81d-6a54-4fa0-ba1b-445aec006f06` — COMPLETED — created 2026-08-26 10:20
- update `2bda4854-0838-4512-86f0-ed524c7e2592` — COMPLETED — created 2026-08-19 22:10


**Data-quality expectations at work: rows dropped between raw and silver by the pipeline's `EXPECT ... ON VIOLATION DROP ROW` rules**

```sql
SELECT 'market_pricing_benchmark' AS dataset, (SELECT count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.raw_market_pricing_benchmark) AS raw_rows, (SELECT count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.silver_market_pricing_benchmark) AS silver_rows
UNION ALL SELECT 'geospatial_hazard_enrichment', (SELECT count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.raw_geospatial_hazard_enrichment), (SELECT count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.silver_geospatial_hazard_enrichment)
UNION ALL SELECT 'credit_bureau_summary', (SELECT count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.raw_credit_bureau_summary), (SELECT count(*) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.silver_credit_bureau_summary)
```
| dataset | raw_rows | silver_rows |
|---|---|---|
| market_pricing_benchmark | 150 | 141 |
| geospatial_hazard_enrichment | 295 | 258 |
| credit_bureau_summary | 500000 | 465730 |

**Expectations declared in the pipeline source** (`src/02_silver/*.sql`):

- `silver_credit_bureau.sql` · **valid_company_id**: `company_id IS NOT NULL` 
- `silver_credit_bureau.sql` · **valid_policy_id**: `policy_id IS NOT NULL` 
- `silver_credit_bureau.sql` · **valid_credit_score**: `credit_score BETWEEN 200 AND 900` 
- `silver_credit_bureau.sql` · **valid_ccj_count**: `ccj_count >= 0` 
- `silver_credit_bureau.sql` · **valid_years_trading**: `years_trading IS NOT NULL AND years_trading >= 0` 
- `silver_geospatial_hazard.sql` · **valid_postcode**: `postcode_sector IS NOT NULL` 
- `silver_geospatial_hazard.sql` · **valid_flood_zone**: `flood_zone_rating BETWEEN 1 AND 10` 
- `silver_geospatial_hazard.sql` · **valid_fire_distance**: `proximity_to_fire_station_km >= 0` 
- `silver_geospatial_hazard.sql` · **valid_crime_index**: `crime_theft_index IS NOT NULL AND crime_theft_index >= 0` 
- `silver_geospatial_hazard.sql` · **valid_subsidence**: `subsidence_risk BETWEEN 0 AND 10` 
- `silver_market_pricing.sql` · **valid_median_rate**: `market_median_rate IS NOT NULL AND market_median_rate > 0` 
- `silver_market_pricing.sql` · **valid_competitor_min**: `competitor_a_min_premium IS NOT NULL AND competitor_a_min_premium > 0` 
- `silver_market_pricing.sql` · **valid_price_trend**: `price_index_trend BETWEEN -50.0 AND 50.0` 
- `silver_market_pricing.sql` · **valid_match_key**: `match_key_sic_region IS NOT NULL` 


## 2. Unity Catalog — governance (tags, comments, lineage, audit)

**Unity Catalog tags on the schema's tables**

```sql
SELECT tag_name, tag_value, count(*) AS tables_tagged
FROM lr_pricing_v2_aws_us_catalog.information_schema.table_tags
WHERE schema_name = 'pricing_workbench_gen2'
GROUP BY tag_name, tag_value ORDER BY tables_tagged DESC LIMIT 12
```
| tag_name | tag_value | tables_tagged |
|---|---|---|
| pw_project | pricing_workbench | 68 |
| pw_managed_by | dab | 68 |
| pw_contains_pii | false | 64 |
| pw_tier | core | 60 |
| pw_owner | laurence.ryszka@databricks.com | 60 |
| pw_environment | demo | 60 |
| demo_environment | true | 7 |
| pw_tier | semantic | 5 |
| business_line | commercial_property | 5 |
| refresh_cadence | realtime | 4 |
| pw_contains_pii | true | 4 |
| pricing_domain | quote_stream | 4 |

**Table descriptions in UC**

```sql
SELECT table_name, comment FROM lr_pricing_v2_aws_us_catalog.information_schema.tables
WHERE table_schema = 'pricing_workbench_gen2' AND table_name IN
 ('unified_motor_table_live','unified_pricing_table_live','optimisation_decision_records','audit_log','postcode_enrichment')
```
| table_name | comment |
|---|---|
| unified_pricing_table_live | PRICING FEATURE TABLE — engineered artifact built from every approved source by the build_upt pipeline. Policies is just one of 8 contributing sources alongside claims, market pricing benchmark, geospatial hazard, credit bureau, real UK postcode enrichment, derived factors, and quotes. policy_id is the grain (one feature vector per policy), not the identity. 50K rows x ~100 columns, PK policy_id (registered UC feature table). Consumed by Frequency/Severity GLMs, GBMs, and the challenger comparison. Can be promoted to the online store (Lakebase) for sub-10ms serving. |
| optimisation_decision_records |  |
| unified_motor_table_live |  |
| postcode_enrichment | ~1.5M real English postcodes built from the ONS Postcode Directory (ONSPD) joined to the English Indices of Deprivation 2019 and the ONS Rural-Urban Classification. Columns include lat/long, LSOA code, IMD deciles (overall + crime/income/health/living environment), is_urban, is_coastal. Built by new_data_impact/00a. |
| audit_log | Immutable audit trail for all Pricing Workbench events — dataset approvals, model decisions, AI agent interactions, factory run submissions. One row per event with a JSON details blob. |

**Lineage into the gold tables (system.access.table_lineage)**

```sql
SELECT source_table_full_name, target_table_full_name, count(*) AS events, max(event_time) AS last_seen
FROM system.access.table_lineage
WHERE target_table_full_name LIKE 'lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_%' AND source_table_full_name IS NOT NULL
GROUP BY ALL ORDER BY events DESC LIMIT 12
```
| source_table_full_name | target_table_full_name | events | last_seen |
|---|---|---|---|
| lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.motor_telematics_aggregate | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_motor_table_live | 5 | 2026-09-13T12:19:35.022Z |
| lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.silver_geospatial_hazard_enrichment | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live | 4 | 2026-08-26T10:42:25.480Z |
| lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.motor_claims_history | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_motor_table_live | 4 | 2026-08-26T10:33:35.353Z |
| lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.silver_credit_bureau_summary | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live | 4 | 2026-08-26T10:42:25.480Z |
| lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_2026_08 | 4 | 2026-08-26T10:43:07.412Z |
| lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.internal_commercial_policies | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live | 4 | 2026-08-26T10:42:25.480Z |
| lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.quotes | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live | 4 | 2026-08-26T10:42:25.480Z |
| lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.silver_market_pricing_benchmark | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live | 4 | 2026-08-26T10:42:25.480Z |
| lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.internal_claims_history | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live | 4 | 2026-08-26T10:42:25.480Z |
| lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.motor_policies | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_motor_table_live | 4 | 2026-08-26T10:33:35.353Z |
| lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.derived_factors | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live | 4 | 2026-08-26T10:42:25.480Z |

**Immutable audit log (app + jobs)**

```sql
SELECT event_type, count(*) AS events, max(timestamp) AS latest
FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.audit_log GROUP BY event_type ORDER BY events DESC LIMIT 12
```
| event_type | events | latest |
|---|---|---|
| model_trained | 144 | 2026-08-26T10:46:35.181Z |
| agent_recommendation | 48 | 2026-10-12T13:21:09.854Z |
| pricing_engine_release_published | 39 | 2026-08-01T09:00:00.000Z |
| optimisation_solve | 19 | 2026-10-13T08:23:25.941Z |
| supervisor_dispatch | 16 | 2026-10-12T13:44:28.037Z |
| notebook_opened | 12 | 2026-10-12T13:26:52.827Z |
| governance_pack_generated | 12 | 2026-08-26T11:00:40.743Z |
| model_promoted | 12 | 2026-08-26T11:00:42.726Z |
| bias_investigation | 11 | 2026-10-12T13:42:22.181Z |
| optimisation_fairness_screen | 9 | 2026-09-05T14:29:35.073Z |
| rating_engine_config_change | 9 | 2026-02-01T09:00:00.000Z |
| factory_plan_approved | 9 | 2026-10-12T13:52:36.960Z |


## 2b. Unity Catalog — semantic layer (metric views)

One governed definition of each KPI, queried with `MEASURE()` by Genie, the dashboard, the agents and the Supervisor. Built by `src/03_gold/build_metric_views.py` (job *Build — semantic layer: UC metric views*).

**Metric views in the schema**

```sql
SELECT table_name, table_type FROM lr_pricing_v2_aws_us_catalog.information_schema.tables
WHERE table_schema = 'pricing_workbench_gen2' AND table_type = 'METRIC_VIEW' ORDER BY table_name
```
| table_name | table_type |
|---|---|
| mv_agent_audit | METRIC_VIEW |
| mv_commercial_portfolio | METRIC_VIEW |
| mv_motor_portfolio | METRIC_VIEW |
| mv_price_frontier | METRIC_VIEW |
| mv_price_optimisation | METRIC_VIEW |
| mv_quote_conversion | METRIC_VIEW |

**mv_motor_portfolio**

```sql
SELECT `Driver Age Band`, MEASURE(`Policies`) AS policies, round(MEASURE(`Pure Risk Cost per Policy-Year`),0) AS risk_cost_per_policy_year,
 round(MEASURE(`Claim Frequency`),3) AS claim_frequency, round(MEASURE(`Average Severity`),0) AS avg_severity
FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.mv_motor_portfolio GROUP BY ALL ORDER BY ALL
```
| Driver Age Band | policies | risk_cost_per_policy_year | claim_frequency | avg_severity |
|---|---|---|---|---|
| 25-70 | 35947 | 811.0 | 0.102 | 7975.0 |
| 70+ | 1619 | 820.0 | 0.103 | 8000.0 |
| U25 | 12434 | 785.0 | 0.099 | 7917.0 |

**mv_quote_conversion**

```sql
SELECT `Driver Age Band`, MEASURE(`Quotes`) AS quotes, round(MEASURE(`Conversion Rate`),3) AS conversion_rate,
 round(MEASURE(`Price vs Technical`),3) AS price_vs_technical FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.mv_quote_conversion GROUP BY ALL ORDER BY ALL
```
| Driver Age Band | quotes | conversion_rate | price_vs_technical |
|---|---|---|---|
| 25-70 | 35947 | 0.67 | 1.007 |
| 70+ | 1619 | 0.665 | 1.007 |
| U25 | 12434 | 0.574 | 1.008 |

**mv_price_optimisation: the headline result as one governed measure**

```sql
SELECT round(MEASURE(`Expected Profit Hold`),0) AS profit_hold, round(MEASURE(`Expected Profit Optimised`),0) AS profit_optimised,
 round(MEASURE(`Profit Uplift`),0) AS profit_uplift, round(MEASURE(`Profit Uplift Pct`),4) AS uplift_pct,
 round(MEASURE(`Book Premium`),0) AS book_premium FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.mv_price_optimisation
```
| profit_hold | profit_optimised | profit_uplift | uplift_pct | book_premium |
|---|---|---|---|---|
| 9419164.0 | 1.0286948E7 | 867784.0 | 0.0921 | 5.324498E7 |


## 3. Lakebase — operational serving (online feature store) + real-time ML scoring

- Lakebase instance `pricing-upt-online-store` — state `AVAILABLE`, capacity `CU_1`
- Lakebase instance `motor-pricing-online-store` — state `AVAILABLE`, capacity `CU_4`

**Sample policy keys (from the gold motor table, synced to Lakebase as `unified_motor_table_live_online`)**

```sql
SELECT policy_id FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_motor_table_live ORDER BY policy_id LIMIT 3
```
| policy_id |
|---|
| POL-MOTOR-00000001 |
| POL-MOTOR-00000002 |
| POL-MOTOR-00000003 |

`pwg2_motor_scorer` ← `{"policy_id": "POL-MOTOR-00000001"}` (features fetched from Lakebase by key) → **[{"final_premium": 1430.88, "freq_pred": 0.5535404804736717, "annual_freq": 0.11070809609473434, "sev_pred": 7161.326277313143, "demand_pred": 0.5469454545171518, "fraud_pred": 0.45616180182916555, "technical_premium": 1075.85, "young_driver_load": 161.38, "telematics_event_load": 107.59, "fraud_load": 86.07, "demand_adj": 0.0, "rating_engine_version": "motor_v1.3", "freq_version": "3", "sev_versi** in 2107 ms

`pwg2_motor_scorer` ← `{"policy_id": "POL-MOTOR-00000002"}` (features fetched from Lakebase by key) → **[{"final_premium": 1120.04, "freq_pred": 0.44372509920015224, "annual_freq": 0.08874501984003044, "sev_pred": 7561.45201007317, "demand_pred": 0.685194196290693, "fraud_pred": 0.22313178401601236, "technical_premium": 910.6, "young_driver_load": 136.59, "telematics_event_load": 0.0, "fraud_load": 72.85, "demand_adj": 0.0, "rating_engine_version": "motor_v1.3", "freq_version": "3", "sev_version": "** in 237 ms

`pwg2_motor_scorer` ← `{"policy_id": "POL-MOTOR-00000003"}` (features fetched from Lakebase by key) → **[{"final_premium": 1207.65, "freq_pred": 0.5703561626474991, "annual_freq": 0.11407123252949983, "sev_pred": 7223.747622264666, "demand_pred": 0.656567662424388, "fraud_pred": 0.27374876198168596, "technical_premium": 1118.2, "young_driver_load": 0.0, "telematics_event_load": 0.0, "fraud_load": 89.46, "demand_adj": 0.0, "rating_engine_version": "motor_v1.3", "freq_version": "3", "sev_version": "3"** in 225 ms


## 4. ML — models registered in Unity Catalog, champion metrics (MLflow)

| model | @champion version | key metrics (from the MLflow run) |
|---|---|---|
| `conversion_elasticity_motor` | v11 | train_auc=0.7715 |
| `demand_gbm` | v36 | auc=0.8268, best_iteration=69.3947, gini=0.6436, logloss=0.4904 |
| `demand_gbm_motor` | v3 | auc=0.6556, best_iteration=39.0, gini=0.3112, logloss=0.4221 |
| `fraud_gbm` | v36 | auc=0.7623, best_iteration=2.0353, gini=0.5246, logloss=0.2884, precision=0.0, recall=0.0 |
| `fraud_gbm_motor` | v3 | auc=0.8691, best_iteration=12.0, gini=0.7381, logloss=0.3818 |
| `freq_glm` | v36 | aic=1299168.5611, bic=-4177914.3588, gini=0.1228, rmse=1.5811 |
| `freq_glm_drift_challenger` | (no champion alias) | |
| `freq_glm_motor` | v3 | aic=7867.9565, bic=-26856.996, gini=0.0389, rmse=0.8914 |
| `governance_agent` | (no champion alias) | |
| `impact_glm_frequency_enriched` | (no champion alias) | |
| `impact_glm_frequency_standard` | (no champion alias) | |
| `pwg2_chat_agent` | (no champion alias) | |
| `pwg2_motor_scorer` | (no champion alias) | |
| `pwg2_motor_scorer_direct` | v3 | (metrics logged as artifacts) |
| `pwg2_pricing_scorer` | (no champion alias) | |
| `retention_elasticity_motor` | v11 | train_auc=0.7029 |
| `sev_glm` | v36 | gini=-0.0754, mae_gbp=46245.4302, r2_log=0.0002 |
| `sev_glm_motor` | v3 | mae=3128.1423, n_train=12257.0 |


## 4b. ML output — price optimisation (constrained solver) results

**Efficient frontier (simulation × solver)**

```sql
SELECT * FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.v_optimisation_frontier ORDER BY 1 LIMIT 8
```
| scenario_id | expected_profit | expected_volume | expected_gwp | pareto |
|---|---|---|---|---|
| cand_00066 | 1.03719325E7 | 25512.6 | 3.010011182E7 | true |
| cand_01559 | 1.021434369E7 | 24482.2 | 2.912802508E7 | true |
| cand_02284 | 1.022583459E7 | 24552.7 | 2.918725304E7 | true |
| hold | 9419164.16 | 32540.1 | 3.458727026E7 | false |

**Solver output: optimised price factors per segment (latest solve, objective = expected_profit)**

```sql
SELECT * FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.optimisation_factor_table LIMIT 10
```
| constraint_version | segment | policies | factor | factor_pct | conversion_hold | conversion_opt | gwp_current | expected_profit_hold | expected_profit_opt | profit_uplift | bound_lo | bound_hi | binding | within_corridor |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| v1 | 25-70 · grpHigh | 7258 | 1.05 | 5.0 | 0.6272 | 0.5862 | 7611717.16 | 1300124.04 | 1438233.58 | 138109.54 | 0.88 | 1.1 | interior | true |
| v1 | 25-70 · grpLow | 16053 | 1.05 | 5.0 | 0.7118 | 0.6752 | 1.706830588E7 | 3308601.66 | 3714698.85 | 406097.19 | 0.88 | 1.1 | interior | true |
| v1 | 25-70 · grpMid | 12636 | 1.05 | 5.0 | 0.6602 | 0.5987 | 1.323414525E7 | 2379400.4 | 2553914.27 | 174513.87 | 0.88 | 1.1 | interior | true |
| v1 | 70+ · grpHigh | 316 | 1.075 | 7.5 | 0.7446 | 0.6409 | 287813.77 | 58362.07 | 64068.34 | 5706.27 | 0.88 | 1.1 | interior | true |
| v1 | 70+ · grpLow | 743 | 1.05 | 5.0 | 0.7256 | 0.6805 | 705408.21 | 139390.66 | 154728.1 | 15337.44 | 0.88 | 1.1 | interior | true |
| v1 | 70+ · grpMid | 560 | 1.0634 | 6.34 | 0.745 | 0.6514 | 519997.92 | 105500.33 | 113738.95 | 8238.62 | 0.88 | 1.1 | interior | true |
| v1 | U25 · grpHigh | 2503 | 0.95 | -5.0 | 0.5006 | 0.6161 | 2748090.99 | 374643.38 | 376427.12 | 1783.75 | 0.88 | 1.06 | interior | true |
| v1 | U25 · grpLow | 5647 | 1.05 | 5.0 | 0.5922 | 0.5604 | 6338446.19 | 1022226.8 | 1144938.01 | 122711.21 | 0.88 | 1.06 | interior | true |
| v1 | U25 · grpMid | 4284 | 0.95 | -5.0 | 0.5673 | 0.6904 | 4731054.95 | 730914.82 | 726200.55 | -4714.27 | 0.88 | 1.06 | interior | true |

**Solve log (audit_log): the objective front door, profit vs volume**

```sql
SELECT timestamp, get_json_object(details,'$.objective') AS objective, round(cast(get_json_object(details,'$.total_uplift') AS double),0) AS profit_uplift_gbp, get_json_object(details,'$.all_within_corridor') AS all_within_corridor FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.audit_log WHERE event_type='optimisation_solve' ORDER BY timestamp DESC LIMIT 6
```
| timestamp | objective | profit_uplift_gbp | all_within_corridor |
|---|---|---|---|
| 2026-10-13T08:23:25.941Z | expected_profit | 867784.0 | true |
| 2026-09-12T14:26:39.674Z | expected_gwp | -1031788.0 | true |
| 2026-09-05T14:28:58.701Z | expected_profit | 867784.0 | true |
| 2026-09-05T14:18:44.375Z | expected_profit | 846702.0 | true |
| 2026-09-05T14:00:06.416Z | expected_profit | 869991.0 | true |
| 2026-09-05T13:51:37.721Z | expected_profit | 876913.0 | true |

**Closed loop: predicted vs realised profit after deployment**

```sql
SELECT advanced_month, round(sum(predicted_profit)/1e6,2) AS predicted_profit_m,
 round(sum(realized_profit)/1e6,2) AS realised_profit_m,
 round(100*(sum(realized_profit)/sum(predicted_profit)-1),2) AS pct_diff
FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.optimisation_advance_result GROUP BY advanced_month
```
| advanced_month | predicted_profit_m | realised_profit_m | pct_diff |
|---|---|---|---|
| 2026-09-01 | 8.79 | 8.77 | -0.2 |

**Red-team: elasticity parameter recovery on synthetic truth**

```sql
SELECT * FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.optimisation_param_recovery LIMIT 6
```
| month_idx | true_slope | recovered_slope | n_quotes |
|---|---|---|---|
| 0 | -6.0 | -5.599 | 3820 |
| 1 | -6.558 | -6.224 | 3773 |
| 2 | -6.988 | -6.652 | 3821 |
| 3 | -7.191 | -6.599 | 3838 |
| 4 | -7.122 | -6.526 | 3842 |
| 5 | -6.796 | -5.705 | 3908 |

**Human-approved decision records (immutable)**

```sql
SELECT decision_id, created_at, approver, constraint_version, objective, get_json_object(chosen_json,'$.expected_profit_hold') AS profit_hold, get_json_object(chosen_json,'$.expected_profit_opt') AS profit_opt, fairness_pass FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.optimisation_decision_records ORDER BY created_at DESC LIMIT 3
```
| decision_id | created_at | approver | constraint_version | objective | profit_hold | profit_opt | fairness_pass |
|---|---|---|---|---|---|---|---|
| 04ee3fe9-0b27-4c36-beb8-31d2333457e4 | 2026-09-05T12:17:17.851Z | laurence.ryszka@databricks.com | v1 | expected_profit | 9315856.36 | 1.018985336E7 | false |
| 25f2ac2b-5375-4b55-ab8e-0609c3c3b24c | 2026-09-05T12:14:47.072Z | laurence.ryszka@databricks.com | v1 | expected_profit | 9315856.36 | 1.018985336E7 | false |
| 35dc9241-ab53-4666-be84-2e19251f4895 | 2026-08-26T07:54:52.931Z | laurence.ryszka@databricks.com | v1 | expected_profit | 7950317.45 | 8787580.84 | true |


## 4c. ML — new-data impact study, re-run live on this workspace

Job *Study — new external data impact on pricing models* (`src/new_data_impact/01_build_all_models.py`): a 200,000-policy synthetic book keyed to real English postcodes; standard vs enriched frequency GLM (reference models).

**Standard vs enriched frequency GLM (test set)**

```sql
SELECT * FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.impact_model_comparison
```
| metric | model_1_standard | model_2_enriched |
|---|---|---|
| AIC | 140488.2857 | 136769.5331 |
| BIC | 140616.3278 | 136966.5211 |
| Deviance | 93093.7322 | 89360.9797 |
| Null Deviance | 94113.4095 | 94113.4095 |
| Deviance Explained | 0.0108 | 0.0505 |
| MAE (test) | 0.3025 | 0.2944 |
| RMSE (test) | 0.4321 | 0.4262 |
| Gini (test) | 0.1134 | 0.2444 |


## 4d. ML — ablation ladder with an out-of-fold leakage guard, and Shapley attribution

Job *Model evidence*, task `challenger_comparison` (`src/04_models/challenger_comparison.py`), on the commercial gold table. The claims-based factor is rebuilt from training-fold claims only before the lift test.

**Sequential ablation ladder (test Gini)**

```sql
SELECT cohort, n_features, round(gini,4) AS gini, round(lift_vs_prev,4) AS lift_vs_prev, attribution_factor, computed_at FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.challenger_comparison_latest
```
| cohort | n_features | gini | lift_vs_prev | attribution_factor | computed_at |
|---|---|---|---|---|---|
| baseline | 21 | 0.0366 | 0.0 |  | 2026-10-13T19:02:34.328Z |
| plus_urban | 22 | 0.0374 | 8.0E-4 | urban_score | 2026-10-13T19:02:34.328Z |
| plus_coastal | 23 | 0.0374 | 0.0 | is_coastal | 2026-10-13T19:02:34.328Z |
| plus_deprivation | 24 | 0.0375 | 1.0E-4 | deprivation_composite | 2026-10-13T19:02:34.328Z |
| plus_claim_freq | 25 | 0.0426 | 0.0051 | neighbourhood_claim_frequency | 2026-10-13T19:02:34.328Z |

**Order-independent Shapley attribution vs the sequential ladder**

```sql
SELECT factor, round(sequential_lift,4) AS sequential_lift, round(shapley_lift,4) AS shapley_lift, round(shapley_share_of_total,3) AS shapley_share, n_subset_fits FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.challenger_attribution_shapley
```
| factor | sequential_lift | shapley_lift | shapley_share | n_subset_fits |
|---|---|---|---|---|
| urban_score | 8.0E-4 | 5.0E-4 | 0.086 | 16 |
| is_coastal | 0.0 | 0.0 | 0.0 | 16 |
| deprivation_composite | 1.0E-4 | 1.0E-4 | 0.014 | 16 |
| neighbourhood_claim_frequency | 0.0051 | 0.0054 | 0.901 | 16 |


## 4e. ML — model assumption checks (every modelling trade-off, measured)

Job *Model evidence*, task `assumption_checks` (`src/04_models/model_assumption_checks.py`).

**model_assumption_checks**

```sql
SELECT check_id, check_name, metric, round(value,5) AS value, detail, verdict FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.model_assumption_checks ORDER BY check_id, metric
```
| check_id | check_name | metric | value | detail | verdict |
|---|---|---|---|---|---|
| 1 | split_stability | baseline_gini_mean | 0.03556 | sd 0.0035 over 20 splits |  |
| 1 | split_stability | full_gini_mean | 0.05319 | sd 0.0033 over 20 splits |  |
| 1 | split_stability | lift_mean | 0.01762 | sd 0.0020, min 0.0139, max 0.0212 | STABLE |
| 2 | leakage_guard | gini_inflation_from_leakage | 0.0126 | mean weight of a policy's own claims in its factor = 0.00093 | MATERIAL: publish the out-of-fold factor |
| 2 | leakage_guard | test_gini_in_sample_factor | 0.05515 | factor built from all policies' claims |  |
| 2 | leakage_guard | test_gini_out_of_fold_factor | 0.04255 | factor built from training-fold claims only |  |
| 3 | credibility_k | credibility_z_median_postcode | 0.91055 | Z = n / (n + K) at K = 100 | K barely binds at this book size |
| 3 | credibility_k | k_estimated_buhlmann_straub |  | EPV 2.5801, VHM -0.000187, 295 postcodes, median 1018 policies each | NO CREDIBLE POSTCODE SIGNAL: VHM ≤ 0, credibility theory gives Z = 0 |
| 3 | credibility_k | k_governed | 100.0 | value used in derive_factors.py |  |
| 3 | credibility_k | test_gini_at_k_0 | 0.04269 | out-of-fold factor |  |
| 3 | credibility_k | test_gini_at_k_10 | 0.04267 | out-of-fold factor |  |
| 3 | credibility_k | test_gini_at_k_100 | 0.04255 | out-of-fold factor |  |
| 3 | credibility_k | test_gini_at_k_1000 | 0.04171 | out-of-fold factor |  |
| 3 | credibility_k | test_gini_at_k_10000 | 0.04052 | out-of-fold factor |  |
| 4 | link_function | frequency_link |  | Poisson log link is the canonical link: no trade-off | CANONICAL |
| 4 | link_function | severity_aic_inverse_canonical | 3110378.62478 | converged=True |  |
| 4 | link_function | severity_aic_log | 3151285.68617 | converged=False |  |
| 4 | link_function | severity_test_gini_inverse_canonical | -0.04096 |  |  |
| 4 | link_function | severity_test_gini_log | -0.04506 |  |  |
| 4 | link_function | share_invalid_predictions_inverse_canonical | 0.0 | linear predictor ≤ 0 means a negative or infinite mean claim cost |  |
| 4 | link_function | share_invalid_predictions_log | 0.0 | linear predictor ≤ 0 means a negative or infinite mean claim cost |  |
| 5 | exposure_offset | intercept_shift | -1.60944 | expected −log(5) = -1.6094 |  |
| 5 | exposure_offset | max_abs_diff_annual_frequency | 0.0 |  | EQUIVALENT while exposure is constant; add the offset when policy terms vary |
| 5 | exposure_offset | max_abs_diff_non_intercept_coefs | 0.0 |  |  |


## 4f. ML — drift-triggered retraining (challenger + human promotion)

Job *Drift-triggered retraining* (`src/04_models/production/drift_detect.py` → condition task → `drift_retrain_challenger.py`), triggered by updates to the raw vendor tables.

**Drift check fired automatically by the table-update trigger (standard gate PSI ≥ 0.2): top features**

```sql
SELECT checked_at, psi_threshold, drift_detected, feature, round(psi,4) AS psi, band, round(baseline_mean,3) AS baseline_mean, round(candidate_mean,3) AS candidate_mean, baseline_source FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.model_drift_metrics WHERE check_run_id = (SELECT max_by(check_run_id, checked_at) FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.model_drift_metrics WHERE psi_threshold = 0.2) ORDER BY psi DESC LIMIT 8
```
| checked_at | psi_threshold | drift_detected | feature | psi | band | baseline_mean | candidate_mean | baseline_source |
|---|---|---|---|---|---|---|---|---|
| 2026-10-13T19:12:50.891Z | 0.2 | false | ccj_count | 0.0568 | stable | 1.094 | 1.121 | stored training profile (lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live current = training data (no data write since champion training; last write v231 at 2026-08-26 10:42 UTC); captured 2026-10-13 18:58 UTC) |
| 2026-10-13T19:12:50.891Z | 0.2 | false | composite_location_risk | 0.0496 | stable | 4.633 | 4.854 | stored training profile (lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live current = training data (no data write since champion training; last write v231 at 2026-08-26 10:42 UTC); captured 2026-10-13 18:58 UTC) |
| 2026-10-13T19:12:50.891Z | 0.2 | false | flood_zone_rating | 0.0411 | stable | 4.019 | 4.301 | stored training profile (lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live current = training data (no data write since champion training; last write v231 at 2026-08-26 10:42 UTC); captured 2026-10-13 18:58 UTC) |
| 2026-10-13T19:12:50.891Z | 0.2 | false | crime_theft_index | 0.0356 | stable | 46.84 | 49.491 | stored training profile (lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live current = training data (no data write since champion training; last write v231 at 2026-08-26 10:42 UTC); captured 2026-10-13 18:58 UTC) |
| 2026-10-13T19:12:50.891Z | 0.2 | false | subsidence_risk | 0.0207 | stable | 4.863 | 5.142 | stored training profile (lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live current = training data (no data write since champion training; last write v231 at 2026-08-26 10:42 UTC); captured 2026-10-13 18:58 UTC) |
| 2026-10-13T19:12:50.891Z | 0.2 | false | credit_score | 0.0019 | stable | 550.073 | 550.883 | stored training profile (lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live current = training data (no data write since champion training; last write v231 at 2026-08-26 10:42 UTC); captured 2026-10-13 18:58 UTC) |
| 2026-10-13T19:12:50.891Z | 0.2 | false | is_coastal | 0.0 | stable | 0.156 | 0.156 | stored training profile (lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live current = training data (no data write since champion training; last write v231 at 2026-08-26 10:42 UTC); captured 2026-10-13 18:58 UTC) |
| 2026-10-13T19:12:50.891Z | 0.2 | false | distance_to_coast_km | 0.0 | stable | 96.325 | 96.325 | stored training profile (lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.unified_pricing_table_live current = training data (no data write since champion training; last write v231 at 2026-08-26 10:42 UTC); captured 2026-10-13 18:58 UTC) |

**Every drift check: gate threshold and decision**

```sql
SELECT checked_at, psi_threshold, max(drift_detected) AS drift_detected, round(max(psi),4) AS max_psi FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.model_drift_metrics GROUP BY check_run_id, checked_at, psi_threshold ORDER BY checked_at DESC LIMIT 6
```
| checked_at | psi_threshold | drift_detected | max_psi |
|---|---|---|---|
| 2026-10-13T19:16:17.399Z | 0.05 | true | 0.0568 |
| 2026-10-13T19:12:50.891Z | 0.2 | false | 0.0568 |
| 2026-10-13T19:02:01.933Z | 0.05 | true | 0.0568 |
| 2026-10-13T18:59:59.001Z | 0.2 | false | 0.0568 |
| 2026-10-13T18:58:31.108Z | 0.2 | false | 0.0 |
| 2026-10-13T18:54:35.979Z | 0.2 | false | 0.0 |

**Retrain log (strict 0.05 gate): challenger vs champion on the same holdout**

```sql
SELECT triggered_at, champion_version, challenger_model, challenger_version, test_rows, split_method, round(champion_gini,4) AS champion_gini, round(challenger_gini,4) AS challenger_gini, round(deviance_change_pct,3) AS deviance_change_pct, round(credibility_k_estimated,0) AS credibility_k_estimated, credibility_flag FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.model_retrain_log ORDER BY triggered_at DESC LIMIT 2
```
| triggered_at | champion_version | challenger_model | challenger_version | test_rows | split_method | champion_gini | challenger_gini | deviance_change_pct | credibility_k_estimated | credibility_flag |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-10-13T19:18:17.726Z | 36 | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.freq_glm_drift_challenger | 2 | 99886 | crc32(policy_id) % 100 < 80 | 0.1203 | 0.1199 | 0.009 | 517472.0 | outside 0.5x-2x of governed K: review the credibility assumption |
| 2026-10-13T19:03:32.258Z | 36 | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.freq_glm_drift_challenger | 1 | 99886 | crc32(policy_id) % 100 < 80 | 0.1203 | 0.1199 | 0.009 | 517472.0 | outside 0.5x-2x of governed K: review the credibility assumption |

**Promotion requests awaiting a named human**

```sql
SELECT created_at, model_name, champion_version, challenger_version, recommendation, substr(rationale,1,200) AS rationale, status FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.model_promotion_requests ORDER BY created_at DESC LIMIT 2
```
| created_at | model_name | champion_version | challenger_version | recommendation | rationale | status |
|---|---|---|---|---|---|---|
| 2026-10-13T19:18:17.726Z | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.freq_glm | 36 | 2 | hold | The champion still holds on the drifted book: Gini Δ -0.0004, deviance +0.01%. Keep monitoring. | pending_human_review |
| 2026-10-13T19:03:32.258Z | lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.freq_glm | 36 | 1 | hold | The champion still holds on the drifted book: Gini Δ -0.0004, deviance +0.01%. Keep monitoring. | pending_human_review |


## 5. GenAI — Agent Framework agent on Claude (live call)

**Question to `pwg2_chat_agent` (persona `recommender`):** Summarise the current optimised motor price plan: which segments move most, the expected profit impact versus holding prices, and whether every segment stays inside the corridor.

**Answer** (85.1s):

> Here is the full decision pack.
> 
> ---
> 
> # 🏷️ Bricksurance SE — Motor Pricing Decision Pack
> 
> ---
> 
> ## ⚠️ RECOMMENDATION: **REVISE BEFORE DEPLOY**
> 
> The optimised factor set delivers a material profit uplift and all segments sit inside the price corridor, but **the fair-value screen has returned an overall FAIL**. Deployment should be paused until the issues below are resolved or formally accepted by the committee with documented rationale.
> 
> ---
> 
> ## 1. Segment Factor Summary & Corridor Status
> 
> | Segment | Factor Move | Conv. Hold → Opt | Profit Uplift (£) | Corridor |
> |---|---|---|---|---|
> | **25-70 · grpLow** | **+5.0 %** | 71.2 % → 67.5 % | **£406,097** ✦ largest | ✅ Inside |
> | **25-70 · grpMid** | +5.0 % | 66.0 % → 59.9 % | £174,514 | ✅ Inside |
> | **25-70 · grpHigh** | +5.0 % | 62.7 % → 58.6 % | £138,110 | ✅ Inside |
> | **U25 · grpLow** | +5.0 % | 59.2 % → 56.0 % | £122,711 | ✅ Inside |
> | **70+ · grpLow** | +5.0 % | 72.6 % → 68.1 % | £15,337 | ✅ Inside |
> | **70+ · grpMid** | +6.34 %** | 74.5 % → 65.1 % | £8,239 | ✅ Inside |
> | **70+ · grpHigh** | **+7.5 %** ✦ largest rise | 74.5 % → 64.1 % | £5,706 | ✅ Inside |
> | **U25 · grpHigh** | **−5.0 %** ✦ only cut | 50.1 % → 61.6 % | £1,784 | ✅ Inside |
> | **U25 · grpMid** | −5.0 % | 56.7 % → 69.0 % | **−£4,714** ⚠️ | ✅ Inside |
> 
> > **All 9 segments are within corridor.** The deployment gate would not block on corridor grounds alone.
> 
> **Key movements:**
> - The **25-70 age band** drives the bulk of the uplift — grpLow alone contributes ~54% of total profit gain.
> - **70+ · grpHigh** receives the steepest increase (+7.5%), which triggers the disparate-impact flag (see §3).
> - **U25 · grpHigh and grpMid** are cut by 5%, stimulating conversion but U25 · grpMid produces a small profit drag (−£4.7k); the committee should confirm this is acceptable.
> 
> ---
> 
> ## 2. Expected Profit vs Hold — Frontier View
> 
> | Scenario | Expected Profit | Volume | GWP |
> |---|---|---|---|
> | **Hold (baseline)** | £9,419,164 | 32,540 | £34,587,270 |
> | Frontier cand_00066 (recommended candidate) | £10,371,933 | 25,513 | £30,100,112 |
> | Frontier cand_02284 | £10,225,835 | 24,553 | £29,187,253 |
> |


## 5b. GenAI — LLM-judge evaluation of the governance Knowledge Assistant (MLflow GenAI)

Job *Governance Assistant — LLM-judge evaluation* (`src/04_models/production/evaluate_governance_assistant.py`). Ground truth is read from Unity Catalog and MLflow, not typed by hand. `baseline` = the same questions to Claude with no documents (the GenAI ablation).

**Pass rate per scorer: no-retrieval baseline vs Knowledge Assistant**

```sql
SELECT * FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.governance_assistant_eval_summary ORDER BY scorer, system
```
| eval_run_id | evaluated_at | system | scorer | pass_rate | n | n_judge_errors | median_latency_ms | retrieval_lift_pp |
|---|---|---|---|---|---|---|---|---|
| f09b11f4-392a-4280-92e4-fcf3a70f2317 | 2026-10-13T18:55:12.628Z | baseline | cites_source | 0.047619047619047616 | 21 | 0 | 4773.0 |  |
| f09b11f4-392a-4280-92e4-fcf3a70f2317 | 2026-10-13T18:55:12.628Z | knowledge_assistant | cites_source | 1.0 | 21 | 0 | 5444.0 | 95.23809523809523 |
| f09b11f4-392a-4280-92e4-fcf3a70f2317 | 2026-10-13T18:55:12.628Z | baseline | correctness | 0.09523809523809523 | 21 | 0 | 4773.0 |  |
| f09b11f4-392a-4280-92e4-fcf3a70f2317 | 2026-10-13T18:55:12.628Z | knowledge_assistant | correctness | 0.7619047619047619 | 21 | 0 | 5444.0 | 66.66666666666666 |
| f09b11f4-392a-4280-92e4-fcf3a70f2317 | 2026-10-13T18:55:12.628Z | baseline | no_deploy_authority | 1.0 | 21 | 0 | 4773.0 |  |
| f09b11f4-392a-4280-92e4-fcf3a70f2317 | 2026-10-13T18:55:12.628Z | knowledge_assistant | no_deploy_authority | 0.9523809523809523 | 21 | 0 | 5444.0 | -4.761904761904767 |
| f09b11f4-392a-4280-92e4-fcf3a70f2317 | 2026-10-13T18:55:12.628Z | baseline | relevance_to_query | 0.42857142857142855 | 21 | 0 | 4773.0 |  |
| f09b11f4-392a-4280-92e4-fcf3a70f2317 | 2026-10-13T18:55:12.628Z | knowledge_assistant | relevance_to_query | 1.0 | 21 | 0 | 5444.0 | 57.14285714285714 |
| f09b11f4-392a-4280-92e4-fcf3a70f2317 | 2026-10-13T18:55:12.628Z | baseline | safety | 1.0 | 21 | 0 | 4773.0 |  |
| f09b11f4-392a-4280-92e4-fcf3a70f2317 | 2026-10-13T18:55:12.628Z | knowledge_assistant | safety | 1.0 | 21 | 0 | 5444.0 | 0.0 |

**Eval set by category**

```sql
SELECT category, count(*) AS questions FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.governance_assistant_eval_set GROUP BY category ORDER BY category
```
| category | questions |
|---|---|
| fairness | 1 |
| guard_rail | 2 |
| intended_use | 1 |
| model_registry | 4 |
| model_specification | 4 |
| pricing_policy | 5 |
| validation_metric | 4 |


## 6. Genie — natural-language Q&A over the governed tables (Conversation API)

**Space:** Modelling Mart — Pricing Q&A (gen2) (`01f19bc879b6126ba41ab0b144c9bf11`)

**Question:** How many commercial policies are there by region, and what is the average annual premium per region?

Generated SQL:
```sql
SELECT
  `Region`,
  MEASURE(`Policies`) AS `Policies`,
  MEASURE(`Average Premium`) AS `Average Premium`
FROM `lr_pricing_v2_aws_us_catalog`.`pricing_workbench_gen2`.`mv_commercial_portfolio`
WHERE `Region` IS NOT NULL
GROUP BY ALL
ORDER BY `Region` ASC
```
| Region | Policies | Average Premium |
|---|---|---|
| London | 131676 | 43427.041754002246 |
| Midlands | 31381 | 43649.74130843504 |
| North West | 44084 | 44132.30274022321 |
| Scotland | 18686 | 43249.002354704055 |
| South West | 9417 | 42108.441223319525 |
| Wales | 6211 | 44324.900982128485 |
| Yorkshire | 15451 | 42594.09403922076 |
Genie: There are **7 regions** in the results, with **London** having the most commercial policies at **131,676** and an average annual premium of **43,427.04**. Examples include:
- **London** — **131,676** policies; average annual premium **43,427.04**
- **North West** — **44,084** policies; average annual premium **44,132.30**
- **Midlands** — **31,381** policies; average annual premium **43,649.74**
- **Scotland** — **18,686** policies; average annual premium **43,249.00**
- **Yorkshire** — **15,451** policies; average annual premium **42,594.09**

Across the regions shown, average annual premiums range from **42,108.44** in **South West** to **44,324.90** in **Wales**, while policy counts range from **6,211** in **Wales** to **131,676** in **London**.


_Status: COMPLETED_

**Genie on the metric views. Question:** What is the expected profit of the optimised motor price plan versus holding prices, by driver age band?

Generated SQL (note `MEASURE()` over the metric view):
```sql
SELECT
  `Driver Age Band`,
  MEASURE(`Expected Profit Hold`) AS `Expected Profit Hold`,
  MEASURE(`Expected Profit Optimised`) AS `Expected Profit Optimised`,
  MEASURE(`Profit Uplift`) AS `Profit Uplift`
FROM `lr_pricing_v2_aws_us_catalog`.`pricing_workbench_gen2`.`mv_price_optimisation`
WHERE `Driver Age Band` IS NOT NULL
GROUP BY ALL
ORDER BY `Driver Age Band` ASC
```
| Driver Age Band | Expected Profit Hold | Expected Profit Optimised | Profit Uplift |
|---|---|---|---|
| 25-70 | 6988126.1 | 7706846.699999999 | 718720.6 |
| 70+ | 303253.06 | 332535.39 | 29282.33 |
| U25 | 2127785.0 | 2247565.6799999997 | 119780.69 |

Genie: The optimised motor price plan shows higher expected profit than holding prices in **all 3 driver age bands**. For **25-70**, expected profit rises from **6,988,126.10** to **7,706,846.70** (**+718,720.60**); for **70+**, it rises from **303,253.06** to **332,535.39** (**+29,282.33**); and for **U25**, it rises from **2,127,785.00** to **2,247,565.68** (**+119,780.69**).

## 6c. Agent Bricks — governance Knowledge Assistant and Pricing Supervisor (live calls)

Built by `src/04_models/production/agent_bricks_setup.py` (job *Build — Agent Bricks*). The Knowledge Assistant's Vector Search index over the governance packs and policy documents is managed by Agent Bricks.

- Knowledge Assistant **Bricksurance Model Governance Assistant (gen2)**: state ``, endpoint `ka-075c42a7-endpoint`
  - source *Model governance packs*: `/Volumes/lr_pricing_v2_aws_us_catalog/pricing_workbench_gen2/governance_packs/`, state `UPDATED`
  - source *Pricing governance policy*: `/Volumes/lr_pricing_v2_aws_us_catalog/pricing_workbench_gen2/governance_docs/`, state `UPDATED`
- Supervisor Agent **Bricksurance Pricing Supervisor (gen2)**: endpoint `mas-041c7524-endpoint`
  - tool `governance_assistant` (knowledge_assistant)
  - tool `pricing_kpis` (genie_space)
  - tool `quote_review` (genie_space)
  - tool `explain_price` (uc_function)

**Question to the governance Knowledge Assistant:** What validation and fairness evidence does the governance pack give for the current frequency model, and which features does it rely on?

**Answer** (10.9s):

> Based on the governance packs provided, here is the validation and fairness evidence for the frequency model:
> 
> ## Validation Evidence
> 
> 
> 
> The model achieves a Gini of 0.138, meaning it ranks policies by expected claim frequency materially better than random, with an acceptance threshold for deployment of 0.20.
> 
>  
> 
> The 5-year actual claim experience strongly supports the pricing direction, with male-led businesses recording an average claim count of 0.979 vs 0.906 for female-led—a 7.9% higher observed frequency, which closely mirrors the model's predicted gap of 7.2%.
> 
>  
> 
> Average incurred losses are also higher for male-led businesses (£21,928 vs £20,137, +8.9%), consistent with the larger sum-insured exposures identified in the proxy analysis.
> 
> 
> 
> ## Fairness Evidence
> 
> 
> 
> Protected-characteristic data (age, sex, ethnicity, disability, religion) is not used as a model feature, and proxies are monitored but not formally audited in the pack—a dedicated fairness review is run quarterly by the Pricing Analytics team in collaboration with Compliance.
> 
>  
> 
> The disparate impact threshold uses the 4/5ths rule on conversion (demand) and on referral rate (fraud).
> 
>  However, 
> 
> director_gender is not listed as a monitored proxy, which is a gap given the premium differential identified.
> 
> 
> 
> ## Key Features
> 
> 
> 
> The proxy path is most clearly visible in two features: industry_risk_tier_encoded (Male: 1.716 vs Female: 1.497, a 15.5% higher tier) and annual_turnover (Male: £1,204,859 vs Female: £944,973, a 27.5% higher turnover). Sum insured follows the same direction (Male: £5.74M vs Female: £4.49M, +27.9%), and CCJ count is also elevated for male directors (1.274 vs 0.920, +38.5%).
> 
>  
> 
> Both turnover and sum insured are legitimate, high-weight rating factors in a frequency GLM.

**Question to the Pricing Supervisor (Agent Bricks):** Is the optimised motor price plan ready to deploy? Give the expected profit vs holding prices from the metric views, and check the pricing constraint policy for what must hold before deployment.

Tools called: `pricing_kpis, governance_assistant`

**Answer** (44.4s):

> I'll help you assess whether the optimised motor price plan is ready to deploy by checking both the expected profit impact and the deployment constraints.
> 
> Let me start by querying the expected profit comparison between optimised and holding prices:
> 
> <name>pricing_kpis</name>
> 
> ||expected_profit_hold|expected_profit_optimised|difference_optimised_vs_hold|
> |-|-|-|-|
> |0|9419164.16|10286947.77|867783.6199999999|
> 
> <name>Bricksurance Pricing Supervisor (gen2)</name>
> 
> Now let me check the pricing constraint policy for deployment requirements:
> 
> <name>Bricksurance Model Governance Assistant (gen2)</name>
> 
> The pricing constraint policy IS the pricing policy, and the solver (optimisation_solver.py) is bound by it; a future agent cannot bypass it — the deployment gate enforces the corridor server-side.
> 
> Before deploying an optimised price plan, several key requirements and constraints must be satisfied:
> 
> **Hard Constraints (Solve-Time Enforced):**
> 
> The deviation corridor and segment caps set the solver's per-segment factor bounds, and the deploy gate re-checks the corridor server-side. A factor outside them cannot be produced or deployed.[^v1Y2-1] Specifically, the deviation corridor never allows pricing below 85% of the technical position or above 115% of the technical position, and breaches are rejected, not warned.[^v1Y2-2]
> 
> Per-segment rate-change caps limit how far the book may move in one optimisation cycle, with a maximum increase of 10.0% and maximum decrease of 12.0% by age-vehicle band.[^v1Y2-3] Additionally, U25 drivers have tighter rate-change caps with a maximum increase of 6.0%.[^v1Y2-4]
> 
> **Renewal Rule (GIPP):**
> 
> The renewal solver prices each renewal as min(prior × factor, equivalent_new_business), so a renewal can NEVER exceed its fresh new-business quote — GIPP holds by construction.[^v1Y2-5]
> 
> **Governance & Approval:**
> 
> The caller must be in `ADMIN_USERS` RBAC.[^v1Y2-6] On top of that the ±corridor is re-checked server-side before any write, so no prompt or agent can bypass it.
> 
> **Audit & Deployment:**
> 
> Every solve and every deploy writes an immutable `audit_log` row; the factor set is stamped to `optimisation_deployment`; constraints are versioned in git.[^v1Y2-7] Any number traces to its constraint version + inputs.
> 
> [^v1Y2-1]: # # ENFORCEMENT MODEL — be honest about HOW each rule below binds (Phase 1, motor # new-business factors): #   * SOLVE-TIME ENFORCED (hard): deviation_corridor + segment_caps — they set the #     solver's per-segment factor bounds, and the deploy gate re-checks the #     corridor server-side. A factor outside them cannot be produced or deployed. [pricing_constraint_policy.md](https://fevm-lr-pricing-v2-aws-us.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/lr_pricing_v2_aws_us_catalog/pricing_workbench_gen2/governance_docs/pricing_constraint_policy.md#:~:text=%23%0A%23%20ENFORCEMENT%20MODEL%20%E2%80%94%20be%20honest%20about%20HOW%20each%20rule%20below%20binds%20%28Phase%201%2C%20motor%0A%23%20new-business%20fac


## 6d. Governance — agent audit trail (every AI explanation, next to the price it reviewed)

Job *Governance — agent audit trail sync* (`src/08_governance/agent_audit_trail.py`): Agent Bricks MLflow traces + Agent Framework AI Gateway inference tables → `v_agent_audit_trail`, KPIs in the `mv_agent_audit` metric view.

**mv_agent_audit**

```sql
SELECT `Agent Layer`, `Agent`, MEASURE(`Interactions`) AS interactions, MEASURE(`Quote-Level Explanations`) AS quote_explanations, MEASURE(`Governance Pack Reviews`) AS pack_reviews, round(MEASURE(`Average Latency Seconds`),1) AS avg_latency_s FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.mv_agent_audit GROUP BY ALL ORDER BY interactions DESC
```
| Agent Layer | Agent | interactions | quote_explanations | pack_reviews | avg_latency_s |
|---|---|---|---|---|---|
| Agent Bricks | Bricksurance Model Governance Assistant (gen2) | 24 | 0 | 1 | 5.6 |
| Agent Framework | pwg2_chat_agent · ask_the_book | 16 | 0 | 0 | 36.3 |
| Agent Framework | pwg2_chat_agent | 14 | 0 | 0 | 42.7 |
| Agent Framework | pwg2_chat_agent · explain | 13 | 0 | 0 | 53.8 |
| Agent Framework | pwg2_chat_agent · bias_investigator | 12 | 0 | 7 | 68.7 |
| Agent Framework | pwg2_chat_agent · rate_change | 8 | 0 | 0 | 59.5 |
| Agent Framework | pwg2_governance_agent | 6 | 0 | 6 | 50.3 |
| Agent Framework | pwg2_chat_agent · recommender | 5 | 0 | 0 | 26.1 |
| Agent Framework | pwg2_chat_agent · drift_monitor | 5 | 0 | 0 | 50.2 |
| Agent Framework | pwg2_chat_agent · model_review | 3 | 0 | 3 | 53.1 |
| Agent Framework | pwg2_chat_agent · price_explainer | 3 | 0 | 0 | 25.7 |
| Agent Framework | pwg2_chat_agent · model_risk_narrator | 2 | 0 | 0 | 22.6 |
| Agent Bricks | Bricksurance Pricing Supervisor (gen2) | 2 | 1 | 0 | 40.8 |
| Agent Framework | pwg2_chat_agent · drift_sentinel | 1 | 0 | 0 | 22.2 |
| Agent Framework | pwg2_chat_agent · committee_scribe | 1 | 0 | 0 | 23.7 |

**An explanation linked to the quote and price plan it reviewed**

```sql
SELECT agent, request_time, reviewed_quote_id, reviewed_quote_technical_premium, reviewed_quote_offered_premium, plan_in_force_objective, round(plan_in_force_profit_uplift,0) AS plan_profit_uplift, tools_called, substr(answer, 1, 300) AS answer_start FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.v_agent_audit_trail WHERE reviewed_quote_id IS NOT NULL ORDER BY request_time DESC LIMIT 3
```
| agent | request_time | reviewed_quote_id | reviewed_quote_technical_premium | reviewed_quote_offered_premium | plan_in_force_objective | plan_profit_uplift | tools_called | answer_start |
|---|---|---|---|---|---|---|---|---|
| Bricksurance Pricing Supervisor (gen2) | 2026-10-13T18:57:21.348Z | MQ-00043750 | 586.02 | 746.22 | expected_profit | 867784.0 | ["examples","lr_pricing_v2_aws_us_catalog__pricing_workbench_gen2__explain_price"] | ## Pricing Breakdown for Motor Quote MQ-00043750

**Customer Profile:**
- Segment: 25-70 · grpMid
- Driver age: 55 years
- Vehicle group: 23

**Technical Price (Break-even):** £586.02
- Calculated using champion models: freq_glm_motor × sev_glm_motor
- This is the pure risk cost before any loadings  |


## 7. Databricks App — surfaced to the business

- App `pricing-workbench-gen2` at https://pricing-workbench-gen2-7474655676955816.aws.databricksapps.com — app `RUNNING`, compute `ACTIVE`, active deployment `01f1af6d181e16e69004ed4e7fda6f68` (SUCCEEDED).
**App/MCP tool calls logged to Unity Catalog**

```sql
SELECT tool, count(*) AS calls FROM lr_pricing_v2_aws_us_catalog.pricing_workbench_gen2.mcp_tool_calls GROUP BY tool ORDER BY calls DESC LIMIT 10
```
| tool | calls |
|---|---|
| record_answers | 8 |
| get_quote_requirements | 4 |
| price_motor_risk | 2 |

