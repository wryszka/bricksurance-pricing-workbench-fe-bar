# Databricks notebook source
# MAGIC %md
# MAGIC # Drift check: is the book still the one the champion was trained on?
# MAGIC
# MAGIC **A platform pattern, not an actuarial model.** The insurer's actuaries own the pricing models. The
# MAGIC framework watches the data every registered champion depends on and, when new data drifts the book,
# MAGIC triggers a challenger retrain with evidence and a promotion request for a named human. The stand-in
# MAGIC model here is the reference commercial frequency GLM (`freq_glm`); any registered model works the same way.
# MAGIC
# MAGIC | Step | What happens |
# MAGIC |---|---|
# MAGIC | **Baseline** | The champion's training data: the gold table *as it was when the champion was trained* (Delta time travel). Falls back to the current table if that version is no longer retained, and records which was used. |
# MAGIC | **Candidate** | The gold table **plus any pending vendor update**: newly arrived `raw_*` vendor rows pushed through the *same* data-quality expectations and derivations as the silver pipeline, then joined exactly as `build_upt.py` does. If nothing is pending, the candidate equals the live table, so one check covers both "data about to be approved" and "data already approved". Written to `drift_candidate_snapshot`. |
# MAGIC | **PSI per model feature** | Population stability index: < 0.1 stable, 0.1 – 0.25 moderate, > 0.25 significant. Written to `model_drift_metrics`. |
# MAGIC | **Gate** | Task values `drift_detected`, `max_psi`, `drifted_features`, `check_run_id` drive the job's condition task. Max PSI ≥ `psi_threshold` → `drift_retrain_challenger.py` trains a challenger. The champion alias is never moved automatically. |

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_pricing_v2_aws_us_catalog")
dbutils.widgets.text("schema_name", "pricing_workbench_gen2")
dbutils.widgets.text("model_name", "freq_glm")                       # registered champion to watch
dbutils.widgets.text("feature_table", "unified_pricing_table_live")  # the gold table the champion trains on
dbutils.widgets.text("psi_threshold", "0.2")
dbutils.widgets.text("include_pending_vendor_updates", "true")       # "false" = compare the live table only

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
fqn = f"{catalog}.{schema}"
model_name = dbutils.widgets.get("model_name")
feature_table = f"{fqn}.{dbutils.widgets.get('feature_table')}"
psi_threshold = float(dbutils.widgets.get("psi_threshold") or 0.2)
include_pending = dbutils.widgets.get("include_pending_vendor_updates").strip().lower() in ("true", "1", "yes")

# COMMAND ----------

import json, uuid, datetime
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                               TimestampType, BooleanType)
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
check_run_id = str(uuid.uuid4())
checked_at = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
user = spark.sql("SELECT current_user()").first()[0]

# Model features: keep in sync with FEATURES in src/04_models/production/freq_glm.py
FEATURES = [
    "sum_insured", "annual_turnover", "current_premium",
    "industry_risk_tier", "construction_type",
    "credit_score", "ccj_count", "years_trading",
    "flood_zone_rating", "proximity_to_fire_station_km",
    "crime_theft_index", "subsidence_risk", "composite_location_risk",
    "urban_score", "is_coastal", "population_density_per_km2",
    "elevation_metres", "annual_rainfall_mm",
    "director_stability_score", "employee_count_est",
    "distance_to_coast_km", "neighbourhood_claim_frequency",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Resolve the champion and its training timestamp

# COMMAND ----------

full_model = f"{fqn}.{model_name}"
champ = w.model_versions.get_by_alias(full_model, "champion")
champion_version = str(champ.version)
trained_at_ms = int(champ.created_at)
source_run_id = champ.run_id
src = champ.source or ""
if src.startswith("models:/m-"):
    # MLflow 3 logged model: its creation time is when the champion was actually trained
    # (replayed versions point at the same logged model).
    try:
        lm = w.api_client.do("GET", f"/api/2.0/mlflow/logged-models/{src.split('/')[-1]}")["model"]["info"]
        trained_at_ms = int(lm.get("creation_timestamp_ms", trained_at_ms))
        source_run_id = lm.get("source_run_id") or source_run_id
    except Exception as e:
        print(f"note: could not read logged model {src}: {str(e)[:120]}")
trained_at = datetime.datetime.fromtimestamp(trained_at_ms / 1000, datetime.timezone.utc).replace(tzinfo=None)
print(f"Champion {full_model} v{champion_version}, trained {trained_at:%Y-%m-%d %H:%M} UTC (run {source_run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Baseline = the champion's training data
# MAGIC
# MAGIC Resolution order, most exact first:
# MAGIC 1. **Stored training profile** in `model_drift_baselines` for this champion version (frozen on its first exact check).
# MAGIC 2. **Current table**, if Delta history shows no data write since the champion was trained (metadata changes only).
# MAGIC 3. **Delta time travel** to the version current at training time (only within the table's file-retention window).
# MAGIC 4. **Current table, flagged approximate**, when none of the above is possible.

# COMMAND ----------

METADATA_OPS = {"CHANGE COLUMN", "SET TBLPROPERTIES", "UNSET TBLPROPERTIES", "ADD CONSTRAINT", "DROP CONSTRAINT",
                "SET TAGS", "UNSET TAGS", "OPTIMIZE", "VACUUM START", "VACUUM END", "ANALYZE", "SET COLUMN TAGS",
                "UNSET COLUMN TAGS", "ALTER COLUMN", "COMMENT"}


def _materialised_ok(df):
    """Force a real file scan (count() can be answered from Delta stats alone)."""
    df.select(F.sum(F.hash(*[col(c) for c in FEATURES]))).collect()
    return df


spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fqn}.model_drift_baselines (
  model_name STRING, champion_version STRING, captured_at TIMESTAMP, baseline_source STRING,
  feature STRING, spec_json STRING, bin STRING, share DOUBLE, mean DOUBLE)
COMMENT 'Frozen training-data profile (bin shares per feature) for each champion version, used as the drift baseline.'
""")
stored = spark.sql(f"""SELECT * FROM {fqn}.model_drift_baselines
                       WHERE model_name = '{model_name}' AND champion_version = '{champion_version}'""").toPandas()

baseline, baseline_exact = None, False
if len(stored):
    baseline_source = f"stored training profile ({stored['baseline_source'].iloc[0]}; captured {stored['captured_at'].iloc[0]:%Y-%m-%d %H:%M} UTC)"
    baseline_exact = True
else:
    hist = spark.sql(f"DESCRIBE HISTORY {feature_table}").select("version", "timestamp", "operation").toPandas()
    hist["timestamp"] = pd.to_datetime(hist["timestamp"], utc=True).dt.tz_convert(None)
    after = hist[(hist["timestamp"] > trained_at) & (~hist["operation"].isin(METADATA_OPS))]
    last_write = hist[(hist["timestamp"] <= trained_at) & (~hist["operation"].isin(METADATA_OPS))].sort_values("version").tail(1)
    if after.empty and len(last_write):
        baseline = spark.table(feature_table)
        lw = f"v{int(last_write['version'].iloc[0])} at {last_write['timestamp'].iloc[0]:%Y-%m-%d %H:%M}" if len(last_write) else "n/a"
        baseline_source = f"{feature_table} current = training data (no data write since champion training; last write {lw} UTC)"
        baseline_exact = True
    else:
        v_train = int(hist[hist["timestamp"] <= trained_at]["version"].max()) if (hist["timestamp"] <= trained_at).any() else 0
        try:
            baseline = _materialised_ok(spark.sql(f"SELECT * FROM {feature_table} VERSION AS OF {v_train}"))
            baseline_source = f"{feature_table} VERSION AS OF {v_train} (as at champion training)"
            baseline_exact = True
        except Exception as e:
            print(f"note: time travel to v{v_train} not possible: {str(e)[:160]}")
            baseline = spark.table(feature_table)
            baseline_source = (f"{feature_table} current (APPROXIMATE: {len(after)} data writes since champion "
                               f"training and the training snapshot is past the retention window)")
print("Baseline:", baseline_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Candidate = gold table + pending vendor update (same DQ rules and joins as the pipeline)

# COMMAND ----------

live = spark.table(feature_table)

GEO_COLS = ["flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index",
            "subsidence_risk", "composite_location_risk", "location_risk_tier"]
BUREAU_COLS = ["credit_score", "ccj_count", "years_trading", "director_changes",
               "credit_risk_tier", "business_stability_score"]
MARKET_COLS = ["market_median_rate", "competitor_a_min_premium", "price_index_trend", "competitor_ratio"]

# Same CONSTRAINT ... EXPECT clauses and derivations as src/02_silver/*.sql
_COMPOSITE = ("(flood_zone_rating * 0.30) + (LEAST(proximity_to_fire_station_km, 25.0) / 25.0 * 10 * 0.20)"
              " + (crime_theft_index / 10.0 * 0.25) + (subsidence_risk * 0.25)")
geo_next = (spark.table(f"{fqn}.raw_geospatial_hazard_enrichment")
    .where("postcode_sector IS NOT NULL")
    .where("flood_zone_rating BETWEEN 1 AND 10")
    .where("proximity_to_fire_station_km >= 0")
    .where("crime_theft_index IS NOT NULL AND crime_theft_index >= 0")
    .where("subsidence_risk BETWEEN 0 AND 10")
    .selectExpr(
        "postcode_sector",
        "CAST(flood_zone_rating AS INT) AS flood_zone_rating",
        "CAST(proximity_to_fire_station_km AS DOUBLE) AS proximity_to_fire_station_km",
        "CAST(crime_theft_index AS DOUBLE) AS crime_theft_index",
        "CAST(subsidence_risk AS DOUBLE) AS subsidence_risk",
        f"ROUND({_COMPOSITE}, 2) AS composite_location_risk",
        f"CASE WHEN ({_COMPOSITE}) >= 6.0 THEN 'High' WHEN ({_COMPOSITE}) >= 3.5 THEN 'Medium' ELSE 'Low' END AS location_risk_tier")
    .dropDuplicates(["postcode_sector"]))

bureau_next = (spark.table(f"{fqn}.raw_credit_bureau_summary")
    .where("company_id IS NOT NULL AND policy_id IS NOT NULL")
    .where("credit_score BETWEEN 200 AND 900")
    .where("ccj_count >= 0")
    .where("years_trading IS NOT NULL AND years_trading >= 0")
    .selectExpr(
        "policy_id",
        "CAST(credit_score AS INT) AS credit_score",
        "CAST(ccj_count AS INT) AS ccj_count",
        "CAST(years_trading AS INT) AS years_trading",
        "CAST(director_changes AS INT) AS director_changes",
        "CASE WHEN credit_score >= 750 AND ccj_count = 0 THEN 'Prime' WHEN credit_score >= 550 AND ccj_count <= 1 THEN 'Standard' "
        "WHEN credit_score >= 400 THEN 'Sub-Standard' ELSE 'High Risk' END AS credit_risk_tier",
        "ROUND(LEAST(100, (LEAST(years_trading, 50) / 50.0 * 40) + (LEAST(credit_score, 900) / 900.0 * 40) "
        "+ (GREATEST(0, 20 - (ccj_count * 5 + director_changes * 3)))), 1) AS business_stability_score")
    .dropDuplicates(["policy_id"]))

market_next = (spark.table(f"{fqn}.raw_market_pricing_benchmark")
    .where("market_median_rate IS NOT NULL AND market_median_rate > 0")
    .where("competitor_a_min_premium IS NOT NULL AND competitor_a_min_premium > 0")
    .where("price_index_trend BETWEEN -50.0 AND 50.0")
    .where("match_key_sic_region IS NOT NULL")
    .selectExpr(
        "match_key_sic_region",
        "CAST(market_median_rate AS DOUBLE) AS market_median_rate",
        "CAST(competitor_a_min_premium AS DOUBLE) AS competitor_a_min_premium",
        "CAST(price_index_trend AS DOUBLE) AS price_index_trend",
        "ROUND(competitor_a_min_premium / NULLIF(market_median_rate, 0), 3) AS competitor_ratio")
    .dropDuplicates(["match_key_sic_region"]))


def changed_keys(next_df, silver_table, key, cols):
    """Keys whose vendor values would change at the next silver refresh (incl. keys added or dropped by DQ)."""
    s = spark.table(f"{fqn}.{silver_table}").select(key, *cols)
    j = next_df.select(key, *cols).alias("n").join(s.alias("s"), key, "full_outer")
    cond = None
    for c in cols:
        e = ~col(f"n.{c}").eqNullSafe(col(f"s.{c}"))
        cond = e if cond is None else (cond | e)
    return j.where(cond).count()


pending = {"geospatial_postcodes": 0, "credit_bureau_policies": 0, "market_keys": 0}
if include_pending:
    pending = {
        "geospatial_postcodes": changed_keys(geo_next, "silver_geospatial_hazard_enrichment", "postcode_sector", GEO_COLS),
        "credit_bureau_policies": changed_keys(bureau_next, "silver_credit_bureau_summary", "policy_id", BUREAU_COLS),
        "market_keys": changed_keys(market_next, "silver_market_pricing_benchmark", "match_key_sic_region", MARKET_COLS),
    }
    cand = live.drop(*GEO_COLS).join(geo_next, "postcode_sector", "left")
    cand = cand.drop(*BUREAU_COLS).join(bureau_next, "policy_id", "left")
    if {"sic_code", "region"} <= set(live.columns):
        cand = (cand.drop(*MARKET_COLS)
                .withColumn("_mk", F.concat(col("sic_code"), lit("_"), col("region")))
                .join(market_next.withColumnRenamed("match_key_sic_region", "_mk"), "_mk", "left")
                .drop("_mk"))
    # Re-derive the gold columns that depend on the overlaid vendor columns (build_upt.py step 8)
    cand = (cand
        .withColumn("market_position_ratio", F.round(
            when(col("market_median_rate").isNotNull() & (col("market_median_rate") > 0),
                 col("rate_per_1k_si") / col("market_median_rate")).otherwise(None), 3))
        .withColumn("combined_risk_score", F.round(
            (F.coalesce(col("composite_location_risk"), lit(5.0)) * 0.35) +
            (F.coalesce(lit(10) - (col("credit_score") - 200) / 70.0, lit(5.0)) * 0.30) +
            (when(col("industry_risk_tier") == "High", 8.0)
             .when(col("industry_risk_tier") == "Medium", 5.0).otherwise(2.5) * 0.20) +
            (when(col("claim_count_5y") > 3, 8.0).when(col("claim_count_5y") > 1, 5.0)
             .when(col("claim_count_5y") > 0, 3.0).otherwise(1.0) * 0.15), 2)))
    cand = cand.select(*live.columns)
    candidate_source = f"{feature_table} + pending vendor update (raw → silver DQ rules → UPT joins)"
else:
    cand = live
    candidate_source = f"{feature_table} (live, no pending updates applied)"

cand_table = f"{fqn}.drift_candidate_snapshot"
cand.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cand_table)
candidate_version = int(spark.sql(f"DESCRIBE HISTORY {cand_table} LIMIT 1").first()["version"])
spark.sql(f"COMMENT ON TABLE {cand_table} IS 'Drift check candidate: the gold pricing table plus any pending vendor update, "
          f"built with the silver data-quality rules. Written by drift_detect.py; read by drift_retrain_challenger.py.'")
print(f"Candidate: {candidate_source}")
print(f"  → {cand_table} v{candidate_version}; pending changes: {pending}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. PSI per model feature (one Spark pass per table, scales to any book size)

# COMMAND ----------

cand_df = spark.table(cand_table).select(*FEATURES)


def build_spec(df):
    """Binning spec from the baseline: categoricals by level, low-cardinality numerics (ratings, counts,
    flags) by value, the rest by baseline deciles."""
    types = dict(df.dtypes)
    cats = [f for f in FEATURES if types[f] in ("string", "boolean")]
    nums = [f for f in FEATURES if f not in cats]
    distinct = df.agg(*[F.approx_count_distinct(c).alias(c) for c in nums]).first().asDict()
    deciles = [c for c in nums if distinct[c] > 20]
    qs = df.approxQuantile(deciles, [i / 10 for i in range(1, 10)], 0.001) if deciles else []
    edges = {c: sorted(set(float(x) for x in q)) for c, q in zip(deciles, qs)}
    return {c: ({"type": "categorical"} if c in cats else
                {"type": "deciles", "edges": edges[c]} if c in edges else {"type": "by_value"}) for c in FEATURES}


def bin_expr(c, s):
    if s["type"] == "deciles":
        e = None
        for i, edge in enumerate(s["edges"]):
            cond = col(c) <= lit(edge)
            e = when(cond, lit(f"d{i:02d}")) if e is None else e.when(cond, lit(f"d{i:02d}"))
        e = e.otherwise(lit(f"d{len(s['edges']):02d}")) if e is not None else lit("d00")
        return when(col(c).isNull(), lit("(null)")).otherwise(e)
    if s["type"] == "by_value":
        return when(col(c).isNull(), lit("(null)")).otherwise(F.format_number(col(c).cast("double"), 4))
    return F.coalesce(col(c).cast("string"), lit("(null)"))


def bin_shares(df, spec):
    long = df.select(F.explode(F.array(*[
        F.struct(lit(c).alias("feature"), bin_expr(c, spec[c]).alias("bin")) for c in FEATURES])).alias("x"))
    counts = long.groupBy("x.feature", "x.bin").count().toPandas()
    counts["share"] = counts["count"] / counts.groupby("feature")["count"].transform("sum")
    return counts[["feature", "bin", "share"]]


def numeric_means(df, spec):
    nums = [c for c in FEATURES if spec[c]["type"] != "categorical"]
    return df.agg(*[F.avg(col(c).cast("double")).alias(c) for c in nums]).first().asDict()


if len(stored):
    spec = {r["feature"]: json.loads(r["spec_json"]) for _, r in stored.drop_duplicates("feature").iterrows()}
    b_shares = stored[["feature", "bin", "share"]]
    means_b = {r["feature"]: r["mean"] for _, r in stored.drop_duplicates("feature").iterrows()}
else:
    base_df = baseline.select(*FEATURES)
    spec = build_spec(base_df)
    b_shares = bin_shares(base_df, spec)
    means_b = numeric_means(base_df, spec)
    if baseline_exact:   # freeze the champion's training profile so later checks don't depend on Delta retention
        prof = b_shares.copy()
        prof["spec_json"] = prof["feature"].map(lambda f: json.dumps(spec[f]))
        prof["mean"] = prof["feature"].map(lambda f: None if means_b.get(f) is None else float(means_b[f]))
        prof_schema = StructType([StructField(n, t) for n, t in [
            ("model_name", StringType()), ("champion_version", StringType()), ("captured_at", TimestampType()),
            ("baseline_source", StringType()), ("feature", StringType()), ("spec_json", StringType()),
            ("bin", StringType()), ("share", DoubleType()), ("mean", DoubleType())]])
        spark.createDataFrame(
            [(model_name, champion_version, checked_at, baseline_source, r["feature"], r["spec_json"], r["bin"],
              float(r["share"]), None if pd.isna(r["mean"]) else float(r["mean"])) for _, r in prof.iterrows()],
            prof_schema).write.mode("append").saveAsTable(f"{fqn}.model_drift_baselines")
        print(f"Froze the training profile for {model_name} v{champion_version} in model_drift_baselines")

c_shares = bin_shares(cand_df, spec)
means_c = numeric_means(cand_df, spec)


def _psi(p, q, eps=1e-4):
    p = np.clip(np.asarray(p, float), eps, None); q = np.clip(np.asarray(q, float), eps, None)
    p, q = p / p.sum(), q / q.sum()
    return float(np.sum((q - p) * np.log(q / p)))


def band(v):
    return "significant" if v > 0.25 else ("moderate" if v >= 0.1 else "stable")


rows = []
for c in FEATURES:
    b = b_shares[b_shares.feature == c].set_index("bin")["share"]
    q = c_shares[c_shares.feature == c].set_index("bin")["share"]
    bins = sorted(set(b.index) | set(q.index))
    v = _psi(b.reindex(bins, fill_value=0).values, q.reindex(bins, fill_value=0).values)
    cat = spec[c]["type"] == "categorical"
    rows.append({
        "feature": c,
        "feature_type": {"categorical": "categorical", "by_value": "numeric (by value)", "deciles": "numeric (deciles)"}[spec[c]["type"]],
        "psi": round(v, 6), "band": band(v),
        "baseline_mean": None if cat or means_b.get(c) is None or pd.isna(means_b.get(c)) else float(means_b[c]),
        "candidate_mean": None if cat or means_c.get(c) is None else float(means_c[c]),
    })
psi_pdf = pd.DataFrame(rows).sort_values("psi", ascending=False).reset_index(drop=True)
max_psi = float(psi_pdf["psi"].max())
drift_detected = max_psi >= psi_threshold
drifted = psi_pdf[psi_pdf["psi"] >= psi_threshold]["feature"].tolist()
display(psi_pdf)
print(f"max PSI = {max_psi:.4f} (threshold {psi_threshold}) → drift_detected = {drift_detected}; drifted: {drifted}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persist the check and hand the decision to the job's condition task

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fqn}.model_drift_metrics (
  check_run_id STRING, checked_at TIMESTAMP, model_name STRING, champion_version STRING,
  baseline_source STRING, candidate_source STRING, feature STRING, feature_type STRING,
  psi DOUBLE, band STRING, baseline_mean DOUBLE, candidate_mean DOUBLE,
  psi_threshold DOUBLE, drift_detected BOOLEAN)
COMMENT 'Population stability index per model feature: champion training data vs the current or pending book. Written by drift_detect.py.'
""")
schema_ = StructType([
    StructField("check_run_id", StringType()), StructField("checked_at", TimestampType()),
    StructField("model_name", StringType()), StructField("champion_version", StringType()),
    StructField("baseline_source", StringType()), StructField("candidate_source", StringType()),
    StructField("feature", StringType()), StructField("feature_type", StringType()),
    StructField("psi", DoubleType()), StructField("band", StringType()),
    StructField("baseline_mean", DoubleType()), StructField("candidate_mean", DoubleType()),
    StructField("psi_threshold", DoubleType()), StructField("drift_detected", BooleanType()),
])
out = [(check_run_id, checked_at, model_name, champion_version, baseline_source, candidate_source,
        r["feature"], r["feature_type"], float(r["psi"]), r["band"],
        None if pd.isna(r["baseline_mean"]) else float(r["baseline_mean"]),
        None if pd.isna(r["candidate_mean"]) else float(r["candidate_mean"]),
        psi_threshold, bool(drift_detected)) for r in psi_pdf.to_dict("records")]
spark.createDataFrame(out, schema_).write.mode("append").saveAsTable(f"{fqn}.model_drift_metrics")

summary = {
    "check_run_id": check_run_id, "model": full_model, "champion_version": champion_version,
    "baseline_source": baseline_source, "candidate_source": candidate_source,
    "candidate_table_version": candidate_version, "pending_vendor_changes": pending,
    "max_psi": round(max_psi, 4), "psi_threshold": psi_threshold,
    "drift_detected": bool(drift_detected), "drifted_features": drifted,
    "top_features": psi_pdf.head(5)[["feature", "psi", "band"]].to_dict("records"),
}
try:
    det = json.dumps(summary, default=str).replace("'", "''")
    spark.sql(f"""
        INSERT INTO {fqn}.audit_log
          (event_id, event_type, entity_type, entity_id, entity_version, user_id, timestamp, details, source)
        SELECT uuid(), 'drift_check', 'model', '{model_name}', '{champion_version}', '{user}',
               current_timestamp(), '{det}', 'job'
    """)
except Exception as e:
    print(f"audit: {e}")

dbutils.jobs.taskValues.set(key="drift_detected", value="true" if drift_detected else "false")
dbutils.jobs.taskValues.set(key="max_psi", value=f"{max_psi:.4f}")
dbutils.jobs.taskValues.set(key="drifted_features", value=",".join(drifted))
dbutils.jobs.taskValues.set(key="check_run_id", value=check_run_id)
print(json.dumps(summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(summary, default=str))
