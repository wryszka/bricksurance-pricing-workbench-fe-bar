# Databricks notebook source
# MAGIC %md
# MAGIC # Semantic layer — Unity Catalog metric views
# MAGIC
# MAGIC One governed definition of every pricing KPI (premium, loss ratio, frequency, severity,
# MAGIC conversion, expected profit), shared by Genie, the AI/BI dashboard, the agents and the app.
# MAGIC Metric views keep the measure logic in Unity Catalog, so "loss ratio" means the same thing
# MAGIC everywhere, and ratios re-aggregate safely at any grouping.
# MAGIC
# MAGIC | Metric view | Source | Answers |
# MAGIC |---|---|---|
# MAGIC | `mv_motor_portfolio` | `unified_motor_table_live` | Motor book: premium, claims, pure risk cost, frequency, severity, telematics |
# MAGIC | `mv_commercial_portfolio` | `unified_pricing_table_live` | Commercial book: premium, sum insured, rate per £1k, loss ratio, peril mix |
# MAGIC | `mv_quote_conversion` | `optimisation_quote_response` | Quote funnel: conversion, price vs technical, price vs market |
# MAGIC | `mv_price_optimisation` | `optimisation_factor_table` | Optimised plan by segment: profit hold vs optimised, conversion, price change, corridor |
# MAGIC | `mv_price_frontier` | `optimisation_scenarios` | Efficient frontier: profit vs volume vs premium across candidate plans |
# MAGIC
# MAGIC Fairness: `gender`, `marital_status` and `occupation_class` are deliberately **not** exposed as
# MAGIC dimensions. They are forbidden pricing signals under the constraint policy
# MAGIC (`optimisation_constraints/default.yaml`).
# MAGIC
# MAGIC Loss-ratio note: claims fields are 5-year totals while `current_premium` is annual, so
# MAGIC 5-year loss ratio = incurred ÷ (premium × 5), and frequency per policy-year = claims ÷ (policies × 5).

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_pricing_v2_aws_us_catalog")
dbutils.widgets.text("schema_name", "pricing_workbench_gen2")
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
fq = f"{catalog}.{schema}"

# COMMAND ----------

METRIC_VIEWS = {
    "mv_motor_portfolio": f"""
version: 1.1
source: {fq}.unified_motor_table_live
comment: "Motor portfolio KPIs (UK private motor, synthetic book). One governed definition of premium, loss ratio, claim frequency and severity."
dimensions:
  - name: Region
    expr: region
  - name: Postcode Area
    expr: postcode_area
  - name: Vehicle Group
    expr: vehicle_group
  - name: Fuel Type
    expr: fuel_type
  - name: Business Use
    expr: business_use
  - name: Driver Age Band
    expr: CASE WHEN driver_age < 25 THEN 'U25' WHEN driver_age < 70 THEN '25-70' ELSE '70+' END
    comment: "Same age bands as the price-optimisation segments"
  - name: No Claims Band
    expr: CASE WHEN no_claims_years = 0 THEN '0' WHEN no_claims_years <= 3 THEN '1-3' WHEN no_claims_years <= 8 THEN '4-8' ELSE '9+' END
  - name: Inception Year
    expr: YEAR(inception_date)
measures:
  - name: Policies
    expr: COUNT(1)
  - name: Written Premium
    expr: SUM(current_premium)
    comment: "Annual written premium (GBP)"
  - name: Average Premium
    expr: AVG(current_premium)
  - name: Claims 5y
    expr: SUM(claim_count_5y)
  - name: Incurred 5y
    expr: SUM(total_incurred_5y)
    comment: "Total incurred claims over 5 years (GBP)"
  - name: Pure Risk Cost per Policy-Year
    expr: SUM(total_incurred_5y) / NULLIF(COUNT(1) * 5, 0)
    comment: "Expected claims cost per policy per year (frequency x severity). The pricing floor. A motor loss ratio is not exposed because the synthetic book's recorded current_premium is stale vs risk cost; price adequacy is measured against the technical premium in mv_quote_conversion instead"
  - name: Claim Frequency
    expr: SUM(claim_count_5y) / NULLIF(COUNT(1) * 5, 0)
    comment: "Claims per policy-year"
  - name: Average Severity
    expr: SUM(total_incurred_5y) / NULLIF(SUM(claim_count_5y), 0)
    comment: "Average cost per claim (GBP)"
  - name: At-fault Share
    expr: SUM(at_fault_count_5y) / NULLIF(SUM(claim_count_5y), 0)
  - name: Telematics Events per Policy
    expr: AVG(telematics_recent_event_count)
""",
    "mv_commercial_portfolio": f"""
version: 1.1
source: {fq}.unified_pricing_table_live
comment: "Commercial property & liability portfolio KPIs (synthetic book with real UK postcode enrichment)."
dimensions:
  - name: Region
    expr: region
  - name: SIC Code
    expr: sic_code
  - name: Construction Type
    expr: construction_type
  - name: Location Risk Tier
    expr: location_risk_tier
  - name: Credit Risk Tier
    expr: credit_risk_tier
  - name: Industry Risk Tier
    expr: industry_risk_tier
  - name: Coastal
    expr: CASE WHEN is_coastal = 1 THEN 'Coastal' ELSE 'Inland' END
measures:
  - name: Policies
    expr: COUNT(1)
  - name: Written Premium
    expr: SUM(current_premium)
  - name: Average Premium
    expr: AVG(current_premium)
  - name: Sum Insured
    expr: SUM(sum_insured)
  - name: Rate per 1k Sum Insured
    expr: SUM(current_premium) * 1000.0 / NULLIF(SUM(sum_insured), 0)
  - name: Incurred 5y
    expr: SUM(total_incurred_5y)
  - name: Loss Ratio 5y
    expr: SUM(total_incurred_5y) / NULLIF(SUM(current_premium) * 5, 0)
    comment: "5-year incurred / (annual premium x 5)"
  - name: Claim Frequency
    expr: SUM(claim_count_5y) / NULLIF(COUNT(1) * 5, 0)
  - name: Average Severity
    expr: SUM(total_incurred_5y) / NULLIF(SUM(claim_count_5y), 0)
  - name: Flood Incurred
    expr: SUM(flood_incurred)
  - name: Fire Incurred
    expr: SUM(fire_incurred)
  - name: Theft Incurred
    expr: SUM(theft_incurred)
  - name: Average Market Position
    expr: AVG(market_position_ratio)
    comment: "Our premium vs market median (1.0 = at market)"
""",
    "mv_quote_conversion": f"""
version: 1.1
source: {fq}.optimisation_quote_response
comment: "Motor quote funnel: conversion and price position vs technical (break-even) and market price."
dimensions:
  - name: Quote Month
    expr: quote_month
  - name: Region
    expr: region
  - name: Driver Age Band
    expr: CASE WHEN driver_age < 25 THEN 'U25' WHEN driver_age < 70 THEN '25-70' ELSE '70+' END
  - name: Vehicle Group
    expr: vehicle_group
  - name: Outcome
    expr: outcome
measures:
  - name: Quotes
    expr: COUNT(1)
  - name: Conversions
    expr: SUM(converted)
  - name: Conversion Rate
    expr: SUM(converted) / NULLIF(COUNT(1), 0)
  - name: Average Offered Premium
    expr: AVG(offered_premium)
  - name: Average Technical Premium
    expr: AVG(technical_premium)
  - name: Price vs Technical
    expr: AVG(vs_technical)
    comment: "Offered / loaded technical price (1.0 = break-even)"
  - name: Price vs Market
    expr: AVG(vs_market)
  - name: Bound Premium
    expr: SUM(offered_premium * converted)
""",
    "mv_price_optimisation": f"""
version: 1.1
source: {fq}.optimisation_factor_table
comment: "Optimised price plan by segment: expected profit at today's prices (hold) vs optimised, conversion and price change, with corridor status. Produced by the constrained solver."
dimensions:
  - name: Segment
    expr: segment
  - name: Driver Age Band
    expr: SPLIT(segment, ' · ')[0]
  - name: Vehicle Group Band
    expr: SPLIT(segment, ' · ')[1]
  - name: Constraint Version
    expr: constraint_version
  - name: Binding Constraint
    expr: binding
  - name: Within Corridor
    expr: within_corridor
measures:
  - name: Policies
    expr: SUM(policies)
  - name: Book Premium
    expr: SUM(gwp_current)
    comment: "Current written premium of the segment (GBP)"
  - name: Expected Profit Hold
    expr: SUM(expected_profit_hold)
    comment: "Expected profit at today's prices"
  - name: Expected Profit Optimised
    expr: SUM(expected_profit_opt)
  - name: Profit Uplift
    expr: SUM(profit_uplift)
  - name: Profit Uplift Pct
    expr: SUM(expected_profit_opt) / NULLIF(SUM(expected_profit_hold), 0) - 1
  - name: Conversion Hold
    expr: SUM(conversion_hold * policies) / NULLIF(SUM(policies), 0)
  - name: Conversion Optimised
    expr: SUM(conversion_opt * policies) / NULLIF(SUM(policies), 0)
  - name: Average Price Change Pct
    expr: SUM(factor_pct * policies) / NULLIF(SUM(policies), 0)
""",
    "mv_price_frontier": f"""
version: 1.1
source: {fq}.optimisation_scenarios
comment: "Efficient frontier of candidate price plans from the simulation: profit vs volume vs premium. 'hold' = today's prices."
dimensions:
  - name: Scenario
    expr: scenario_id
  - name: On Frontier
    expr: pareto
  - name: Is Hold Plan
    expr: scenario_id = 'hold'
measures:
  - name: Candidate Plans
    expr: COUNT(1)
  - name: Expected Profit
    expr: MAX(expected_profit)
  - name: Expected GWP
    expr: MAX(expected_gwp)
    comment: "Conversion-weighted expected written premium"
  - name: Expected Volume
    expr: MAX(expected_volume)
  - name: Expected Loss Ratio
    expr: AVG(expected_loss_ratio)
""",
}

# COMMAND ----------

import re

built = []
for name, yaml in METRIC_VIEWS.items():
    source = re.search(r"^source:\s*(\S+)", yaml, re.M).group(1)
    if not spark.catalog.tableExists(source):
        # e.g. optimisation_* tables only exist when enable_optimization=true
        print(f"– skip {name}: source {source} not built in this deployment")
        continue
    spark.sql(f"CREATE OR REPLACE VIEW {fq}.{name} WITH METRICS LANGUAGE YAML AS $${yaml}$$")
    built.append(name)
    print(f"✓ metric view {fq}.{name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — query each metric view with MEASURE()

# COMMAND ----------

display(spark.sql(f"""
SELECT `Driver Age Band`, MEASURE(`Policies`) AS policies, MEASURE(`Written Premium`) AS written_premium,
       MEASURE(`Pure Risk Cost per Policy-Year`) AS risk_cost, MEASURE(`Claim Frequency`) AS claim_frequency
FROM {fq}.mv_motor_portfolio GROUP BY ALL ORDER BY ALL"""))
display(spark.sql(f"""
SELECT `Region`, MEASURE(`Written Premium`) AS written_premium, MEASURE(`Loss Ratio 5y`) AS loss_ratio_5y
FROM {fq}.mv_commercial_portfolio GROUP BY ALL ORDER BY ALL"""))
if "mv_quote_conversion" in built:
    display(spark.sql(f"""
SELECT `Driver Age Band`, MEASURE(`Conversion Rate`) AS conversion_rate, MEASURE(`Price vs Technical`) AS price_vs_technical
FROM {fq}.mv_quote_conversion GROUP BY ALL ORDER BY ALL"""))
if "mv_price_optimisation" in built:
    display(spark.sql(f"""
SELECT `Driver Age Band`, MEASURE(`Expected Profit Hold`) AS profit_hold, MEASURE(`Expected Profit Optimised`) AS profit_opt,
       MEASURE(`Profit Uplift Pct`) AS uplift_pct, MEASURE(`Average Price Change Pct`) AS price_change_pct
FROM {fq}.mv_price_optimisation GROUP BY ALL ORDER BY ALL"""))
if "mv_price_frontier" in built:
    display(spark.sql(f"""
SELECT `Scenario`, `On Frontier`, MEASURE(`Expected Profit`) AS expected_profit, MEASURE(`Expected Volume`) AS expected_volume
FROM {fq}.mv_price_frontier WHERE `On Frontier` OR `Is Hold Plan` GROUP BY ALL ORDER BY expected_profit DESC LIMIT 5"""))

# COMMAND ----------

for name in built:
    spark.sql(f"ALTER VIEW {fq}.{name} SET TAGS ('pw_project'='pricing_workbench', 'pw_tier'='semantic', 'pw_managed_by'='dab', 'pw_contains_pii'='false')")
print("✓ tagged")
