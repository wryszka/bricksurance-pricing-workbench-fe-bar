# Databricks notebook source
# MAGIC %md
# MAGIC # Challenger Comparison — does each new real-data factor earn its place?
# MAGIC
# MAGIC Answers the business question: **does adding the derived factors actually
# MAGIC make the model better?** Trains five Poisson GLMs for claim frequency, each
# MAGIC an ablation of the real-UK-data factor set, and reports Gini on a held-out
# MAGIC sample plus per-factor lift attribution.
# MAGIC
# MAGIC The GLMs are **reference models on a synthetic book**. The point is the platform
# MAGIC pattern: any new data source gets a governed, reproducible lift test logged to MLflow
# MAGIC and Unity Catalog before an actuary decides whether to use it.
# MAGIC
# MAGIC **Ablation ladder:**
# MAGIC | # | Feature set                                        | Cohort tag         |
# MAGIC |---|----------------------------------------------------|--------------------|
# MAGIC | 1 | Baseline features only                             | `baseline`         |
# MAGIC | 2 | + `urban_score` (ONS RUC + IMD living environment) | `plus_urban`       |
# MAGIC | 3 | + `is_coastal` (ONS local-authority flag)          | `plus_coastal`     |
# MAGIC | 4 | + `deprivation_composite` (IMD domains)            | `plus_deprivation` |
# MAGIC | 5 | + `neighbourhood_claim_frequency` (credibility)    | `plus_claim_freq`  |
# MAGIC
# MAGIC **Attribution, two ways:**
# MAGIC - *Sequential* (the ladder): each factor's lift = Gini(with it) − Gini(previous cohort).
# MAGIC   Simple to read, but **order-dependent**: correlated factors (urban areas also claim
# MAGIC   more) give the shared signal to whichever factor enters first.
# MAGIC - *Shapley* (order-independent): each factor's average marginal Gini lift over every
# MAGIC   subset of the other real-data factors (2⁴ = 16 fits). Written next to the sequential
# MAGIC   numbers, so the ordering trade-off is measured rather than assumed.
# MAGIC
# MAGIC **Leakage guard:** `neighbourhood_claim_frequency` is built from claims, so it is
# MAGIC recomputed from training-fold claims only before the lift test.
# MAGIC
# MAGIC **Outputs:**
# MAGIC - Five MLflow runs (one per cohort) with tag `challenger_cohort`
# MAGIC - Summary table `challenger_comparison_latest` consumed by the app
# MAGIC - `challenger_attribution_shapley`: sequential vs Shapley lift per factor

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_pricing_v2_aws_us_catalog")
dbutils.widgets.text("schema_name", "pricing_workbench_gen2")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
fqn = f"{catalog}.{schema}"

# COMMAND ----------

import itertools, math, zlib
import mlflow
import numpy as np
import pandas as pd
import statsmodels.api as sm
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

try:
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    mlflow.set_experiment("/Workspace/Shared/.bundle/pricing-workbench-gen2/experiments/challenger_comparison")
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load UPT and prepare training frame

# COMMAND ----------

upt_table_name = f"{fqn}.unified_pricing_table_live"
upt = spark.table(upt_table_name)

# Delta version for reproducibility
upt_history = spark.sql(f"DESCRIBE HISTORY {upt_table_name} LIMIT 1").collect()
upt_delta_version = upt_history[0]["version"] if upt_history else None

# Baseline feature set — same shape as model_01_glm_frequency.py, MINUS the derived factors
BASELINE_FEATURES = [
    "annual_turnover", "sum_insured", "building_age_years", "current_premium",
    "flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index", "subsidence_risk",
    "composite_location_risk", "credit_score", "ccj_count", "years_trading",
    "business_stability_score", "market_median_rate",
    "credit_default_probability", "director_stability_score",
    "employee_count_est", "distance_to_coast_km", "population_density_per_km2",
    "elevation_metres", "annual_rainfall_mm",
]

# Real-UK-data derived factors, added in ablation order
REAL_FACTORS = [
    "urban_score",             # ONS RUC 2011 + IMD living-env
    "is_coastal",              # ONS local authority coastal flag
    "deprivation_composite",   # IMD crime + income + health + living-env
    "neighbourhood_claim_frequency",  # Bühlmann credibility on internal claims
]

all_cols = ["policy_id", "postcode_sector", "claim_count_5y"] + BASELINE_FEATURES + REAL_FACTORS

# Check the derived factors exist in the UPT; fail loudly if not
upt_cols = set(upt.columns)
missing = [c for c in REAL_FACTORS if c not in upt_cols]
if missing:
    raise ValueError(
        f"Derived factors {missing} not in UPT. "
        f"Run 03_gold/derive_factors.py then 03_gold/build_upt.py before this notebook."
    )

pdf = upt.select(*all_cols).toPandas()
pdf["claim_frequency"] = pdf["claim_count_5y"].fillna(0).astype(float)
for c in BASELINE_FEATURES + REAL_FACTORS:
    pdf[c] = pd.to_numeric(pdf[c], errors="coerce").fillna(0)

# Deterministic 80/20 split by policy_id. zlib.crc32 is stable across processes;
# Python's built-in hash() is salted per process (PYTHONHASHSEED), which made the
# earlier split differ on every run.
pdf["split_hash"] = pdf["policy_id"].apply(lambda x: zlib.crc32(str(x).encode()) % 100)
train_pdf = pdf[pdf["split_hash"] < 80].copy()
test_pdf  = pdf[pdf["split_hash"] >= 80].copy()
# Leakage guard: neighbourhood_claim_frequency is built from claims, so the copy in the
# UPT includes the holdout policies' own claims. For an honest lift test it is rebuilt
# here from TRAINING-fold claims only (same Bühlmann K as derive_factors.py), so no
# holdout label feeds a feature. model_assumption_checks.py measures what the
# in-sample version would have inflated.
K_CREDIBILITY = 100
_overall = train_pdf["claim_frequency"].mean()
_pc = train_pdf.groupby("postcode_sector")["claim_frequency"].agg(["count", "mean"])
_z = _pc["count"] / (_pc["count"] + K_CREDIBILITY)
_oof = _z * _pc["mean"] + (1 - _z) * _overall
for _df in (train_pdf, test_pdf):
    _df["neighbourhood_claim_frequency"] = _df["postcode_sector"].map(_oof).fillna(_overall).values

y_train = train_pdf["claim_frequency"].values
y_test  = test_pdf["claim_frequency"].values

print(f"Train: {len(train_pdf)}, Test: {len(test_pdf)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gini helper

# COMMAND ----------

def gini(y_true, y_pred):
    """Normalized Gini coefficient — insurance-standard uplift metric.
    Ranges from ~0 (no rank-order signal) to ~1 (perfect rank-order)."""
    arr = np.asarray(list(zip(y_true, y_pred, np.arange(len(y_true)))), dtype=float)
    # Sort by prediction desc; tie-break on original index
    arr = arr[np.lexsort((arr[:, 2], -arr[:, 1]))]
    total_pos = arr[:, 0].sum()
    if total_pos == 0:
        return 0.0
    cum_pos = np.cumsum(arr[:, 0]) / total_pos
    # Lorenz area vs diagonal
    giniSum = cum_pos.sum() - (len(arr) + 1) / 2.0
    raw = giniSum / len(arr)
    # Normalize by perfect model
    arr_perf = np.sort(y_true)[::-1]
    cum_perf = np.cumsum(arr_perf) / total_pos
    perfSum = cum_perf.sum() - (len(arr_perf) + 1) / 2.0
    perf = perfSum / len(arr_perf)
    if perf == 0:
        return 0.0
    return float(raw / perf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit the five cohorts and collect results

# COMMAND ----------

# Each cohort adds one real-data factor on top of the previous — letting us attribute
# lift factor-by-factor via the Gini delta between consecutive cohorts. The last
# cohort `plus_claim_freq` carries the full real-data feature set.
COHORTS = [
    ("baseline",         BASELINE_FEATURES,                               None),
    ("plus_urban",       BASELINE_FEATURES + REAL_FACTORS[:1],            REAL_FACTORS[0]),
    ("plus_coastal",     BASELINE_FEATURES + REAL_FACTORS[:2],            REAL_FACTORS[1]),
    ("plus_deprivation", BASELINE_FEATURES + REAL_FACTORS[:3],            REAL_FACTORS[2]),
    ("plus_claim_freq",  BASELINE_FEATURES + REAL_FACTORS[:4],            REAL_FACTORS[3]),
]

results = []
run_ids = {}

for cohort_name, features, attribution in COHORTS:
    X_train = sm.add_constant(train_pdf[features].values)
    X_test  = sm.add_constant(test_pdf[features].values)

    with mlflow.start_run(run_name=f"challenger_{cohort_name}") as run:
        mlflow.set_tag("challenger_cohort",    cohort_name)
        mlflow.set_tag("challenger_comparison", "true")
        mlflow.set_tag("feature_table",         upt_table_name)
        mlflow.log_param("model_type",        "GLM_Poisson")
        mlflow.log_param("n_features",        len(features))
        mlflow.log_param("cohort",            cohort_name)
        mlflow.log_param("attribution_factor", attribution)
        mlflow.log_param("upt_delta_version", upt_delta_version)
        mlflow.log_param("train_rows",        len(train_pdf))
        mlflow.log_param("test_rows",         len(test_pdf))
        mlflow.log_param("features",          ",".join(features))

        glm = sm.GLM(y_train, X_train, family=sm.families.Poisson(link=sm.families.links.Log()))
        fit = glm.fit()
        y_pred = fit.predict(X_test)

        g = gini(y_test, y_pred)
        rmse = float(np.sqrt(np.mean((y_test - y_pred) ** 2)))

        mlflow.log_metric("gini",           g)
        mlflow.log_metric("rmse",           rmse)
        mlflow.log_metric("aic",            float(fit.aic))

        run_ids[cohort_name] = run.info.run_id
        results.append({
            "cohort":              cohort_name,
            "n_features":          len(features),
            "gini":                g,
            "rmse":                rmse,
            "aic":                 float(fit.aic),
            "run_id":              run.info.run_id,
            "attribution_factor":  attribution,
        })
        print(f"  {cohort_name:18s}  features={len(features):3d}  Gini={g:.4f}  RMSE={rmse:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attribution — per-factor lift via consecutive Gini deltas

# COMMAND ----------

gini_baseline = results[0]["gini"]
gini_full     = results[-1]["gini"]
lift_total    = gini_full - gini_baseline
lift_total_pct = (lift_total / gini_baseline * 100.0) if gini_baseline > 0 else 0.0

print(f"Baseline Gini:    {gini_baseline:.4f}")
prev_gini = gini_baseline
for r in results[1:]:
    lift_prev = r["gini"] - prev_gini
    print(f"+ {r['attribution_factor']:30s}  Gini={r['gini']:.4f}  (+{lift_prev:+.4f})")
    prev_gini = r["gini"]
print(f"Total lift:       {lift_total:+.4f}  ({lift_total_pct:+.2f}% vs baseline)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write summary table consumed by the app

# COMMAND ----------

summary_schema = StructType([
    StructField("cohort",               StringType()),
    StructField("n_features",           IntegerType()),
    StructField("gini",                 DoubleType()),
    StructField("rmse",                 DoubleType()),
    StructField("aic",                  DoubleType()),
    StructField("lift_vs_baseline",     DoubleType()),
    StructField("lift_vs_prev",         DoubleType()),
    StructField("attribution_factor",   StringType()),
    StructField("run_id",               StringType()),
    StructField("upt_delta_version",    StringType()),
    StructField("computed_at",          TimestampType()),
])

now = datetime.now(timezone.utc).replace(tzinfo=None)
summary_rows = []
prev_gini = gini_baseline
for r in results:
    lift_vs_baseline = r["gini"] - gini_baseline
    lift_vs_prev     = r["gini"] - prev_gini
    summary_rows.append((
        r["cohort"], r["n_features"], r["gini"], r["rmse"], r["aic"],
        lift_vs_baseline, lift_vs_prev, r["attribution_factor"],
        r["run_id"], str(upt_delta_version), now,
    ))
    prev_gini = r["gini"]

summary_df = spark.createDataFrame(summary_rows, schema=summary_schema)

summary_table = f"{fqn}.challenger_comparison_latest"
summary_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(summary_table)

spark.sql(f"""
    ALTER TABLE {summary_table}
    SET TBLPROPERTIES (
        'comment' = 'Latest challenger vs baseline comparison — consumed by the Pricing Workbench app.'
    )
""")
print(f"✓ {summary_table} — {summary_df.count()} rows")
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order-independent attribution (Shapley) vs the sequential ladder
# MAGIC
# MAGIC Fits the baseline plus every subset of the four real-data factors on the same
# MAGIC split, then averages each factor's marginal Gini lift over all orderings.
# MAGIC The Shapley values sum to the same total lift as the ladder, so the two
# MAGIC attributions are directly comparable.

# COMMAND ----------

def _fit_gini(features):
    fit = sm.GLM(y_train, sm.add_constant(train_pdf[features].values),
                 family=sm.families.Poisson(link=sm.families.links.Log())).fit()
    return gini(y_test, fit.predict(sm.add_constant(test_pdf[features].values)))

subset_gini = {}
for r_ in range(len(REAL_FACTORS) + 1):
    for combo in itertools.combinations(REAL_FACTORS, r_):
        subset_gini[frozenset(combo)] = _fit_gini(BASELINE_FEATURES + list(combo))

n_f = len(REAL_FACTORS)
shapley = {}
for f_ in REAL_FACTORS:
    others = [x for x in REAL_FACTORS if x != f_]
    val = 0.0
    for r_ in range(len(others) + 1):
        for combo in itertools.combinations(others, r_):
            weight = math.factorial(r_) * math.factorial(n_f - r_ - 1) / math.factorial(n_f)
            val += weight * (subset_gini[frozenset(combo) | {f_}] - subset_gini[frozenset(combo)])
    shapley[f_] = val

sequential = {r["attribution_factor"]: r["gini"] - results[i - 1]["gini"]
              for i, r in enumerate(results) if r["attribution_factor"]}
total_lift_all = subset_gini[frozenset(REAL_FACTORS)] - subset_gini[frozenset()]

attr_rows = [(f_, float(sequential[f_]), float(shapley[f_]),
              float(shapley[f_] / total_lift_all) if total_lift_all else None,
              len(subset_gini), str(upt_delta_version), now)
             for f_ in REAL_FACTORS]
attr_df = spark.createDataFrame(attr_rows, schema=StructType([
    StructField("factor", StringType()), StructField("sequential_lift", DoubleType()),
    StructField("shapley_lift", DoubleType()), StructField("shapley_share_of_total", DoubleType()),
    StructField("n_subset_fits", IntegerType()), StructField("upt_delta_version", StringType()),
    StructField("computed_at", TimestampType())]))
attr_table = f"{fqn}.challenger_attribution_shapley"
attr_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(attr_table)
spark.sql(f"COMMENT ON TABLE {attr_table} IS 'Per-factor Gini lift: sequential ablation ladder vs order-independent Shapley attribution over all 16 factor subsets'")

with mlflow.start_run(run_name="challenger_attribution_shapley"):
    mlflow.set_tag("challenger_comparison", "true")
    mlflow.log_param("n_subset_fits", len(subset_gini))
    mlflow.log_param("upt_delta_version", upt_delta_version)
    for f_ in REAL_FACTORS:
        mlflow.log_metric(f"shapley_lift_{f_}", shapley[f_])
        mlflow.log_metric(f"sequential_lift_{f_}", sequential[f_])

print(f"Total lift (baseline → all real-data factors): {total_lift_all:+.4f}")
print(f"{'factor':32s} {'sequential':>11s} {'shapley':>9s}")
for f_ in REAL_FACTORS:
    print(f"{f_:32s} {sequential[f_]:+11.4f} {shapley[f_]:+9.4f}")
display(attr_df)
