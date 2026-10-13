# Databricks notebook source
# MAGIC %md
# MAGIC # Drift-triggered challenger retrain + human promotion request
# MAGIC
# MAGIC Runs only when `drift_detect.py` finds drift above the threshold (the job's condition task gates it).
# MAGIC **A platform pattern, not an actuarial model:** the framework refits the *same specification* the
# MAGIC insurer's actuaries registered, on the drifted book, and produces the evidence a human needs to decide.
# MAGIC Stand-in model: the reference commercial frequency GLM (`freq_glm`).
# MAGIC
# MAGIC 1. Refit the champion's specification (Poisson GLM, log link, `claim_count_5y`, same encoding as
# MAGIC    `freq_glm.py`) on `drift_candidate_snapshot`, with a **stable** split (CRC32 of `policy_id`), so
# MAGIC    re-runs reproduce the same holdout.
# MAGIC 2. Score the **champion** on the *same* holdout rows with its own logged coefficients
# MAGIC    (`relativities.csv` from its training run), so champion and challenger are compared like for like.
# MAGIC 3. Register the challenger as a **separate** UC model `freq_glm_drift_challenger` (alias `challenger`).
# MAGIC    The production model and its `@champion` alias are never touched.
# MAGIC 4. Write `model_retrain_log`, a `model_promotion_requests` row with status `pending_human_review`,
# MAGIC    and an `audit_log` event. Promotion is a named human's decision.
# MAGIC 5. Monitored assumption: re-estimate the Bühlmann–Straub credibility constant K for
# MAGIC    `neighbourhood_claim_frequency` on the candidate and log it next to the governed K = 100.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_pricing_v2_aws_us_catalog")
dbutils.widgets.text("schema_name", "pricing_workbench_gen2")
dbutils.widgets.text("model_name", "freq_glm")
dbutils.widgets.text("challenger_model_name", "freq_glm_drift_challenger")
dbutils.widgets.text("check_run_id", "")          # from detect_drift task values; empty = latest check
dbutils.widgets.text("max_psi", "")
dbutils.widgets.text("drifted_features", "")
dbutils.widgets.text("governed_credibility_k", "100")
dbutils.widgets.text("freq_exposure_years", "5")  # claim_count_5y covers a 5-year window (see freq_glm.py)
dbutils.widgets.text("experiment_path", "/Workspace/Shared/.bundle/pricing-workbench-gen2/experiments/drift_retraining")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
fqn = f"{catalog}.{schema}"
model_name = dbutils.widgets.get("model_name")
challenger_name = dbutils.widgets.get("challenger_model_name")
check_run_id = dbutils.widgets.get("check_run_id").strip()
max_psi = dbutils.widgets.get("max_psi").strip()
drifted_features = dbutils.widgets.get("drifted_features").strip()
governed_k = float(dbutils.widgets.get("governed_credibility_k") or 100)
exposure_years = float(dbutils.widgets.get("freq_exposure_years") or 5)
experiment_path = dbutils.widgets.get("experiment_path")

# COMMAND ----------

import json, uuid, zlib, datetime, tempfile, os
import numpy as np
import pandas as pd
import statsmodels.api as sm
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature
import pyspark.sql.functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, LongType, TimestampType)
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()
user = spark.sql("SELECT current_user()").first()[0]
now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

# Same rating-factor subset as the champion (src/04_models/production/freq_glm.py FEATURES)
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
TARGET, KEY = "claim_count_5y", "policy_id"

# If run by hand (no task values), use the latest drift check.
if not check_run_id:
    last = spark.sql(f"""SELECT check_run_id, max(psi) AS max_psi,
                                concat_ws(',', collect_list(CASE WHEN psi >= psi_threshold THEN feature END)) AS drifted
                         FROM {fqn}.model_drift_metrics
                         GROUP BY check_run_id ORDER BY max(checked_at) DESC LIMIT 1""").first()
    check_run_id, max_psi, drifted_features = last["check_run_id"], f"{last['max_psi']:.4f}", last["drifted"]
print(f"Drift check {check_run_id}: max PSI {max_psi}; drifted features: {drifted_features or '(none)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Candidate book + stable split + the champion's own encoding

# COMMAND ----------

cand_table = f"{fqn}.drift_candidate_snapshot"
candidate_version = int(spark.sql(f"DESCRIBE HISTORY {cand_table} LIMIT 1").first()["version"])
pdf = spark.table(cand_table).select(KEY, TARGET, "postcode_sector", *FEATURES).toPandas()
y = pdf[TARGET].fillna(0).astype(float)


def _prep_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Identical to freq_glm.py: object → string-or-'(null)', numeric → float with NaN → 0."""
    out = df[FEATURES].copy()
    for c in out.columns:
        if out[c].dtype == "object":
            out[c] = out[c].astype(str).where(out[c].notna(), "(null)")
        else:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0).astype(float)
    return out


raw_X = _prep_raw(pdf)
X = pd.get_dummies(raw_X, drop_first=True, dtype=float).fillna(0.0)
# Stable split: CRC32 is the same in every process (Python's built-in hash() of a str is not).
bucket = pdf[KEY].astype(str).map(lambda s: zlib.crc32(s.encode("utf-8")) % 100)
train = (bucket < 80).values
X_train, y_train, X_test, y_test = X[train], y[train], X[~train], y[~train]
print(f"Candidate v{candidate_version}: {len(pdf):,} rows · train {train.sum():,} · holdout {(~train).sum():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fit the challenger (same specification as the champion)

# COMMAND ----------

res = sm.GLM(y_train.values, sm.add_constant(X_train.values, has_constant="add"),
             family=sm.families.Poisson(link=sm.families.links.Log())).fit(maxiter=50)
feature_names = list(X.columns)
mu_chal = res.predict(sm.add_constant(X_test.values, has_constant="add"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Score the champion on the same holdout with its own logged coefficients

# COMMAND ----------

champ = w.model_versions.get_by_alias(f"{fqn}.{model_name}", "champion")
champion_version = str(champ.version)
source_run_id = champ.run_id
src = champ.source or ""
if src.startswith("models:/m-"):
    try:
        lm = w.api_client.do("GET", f"/api/2.0/mlflow/logged-models/{src.split('/')[-1]}")["model"]["info"]
        source_run_id = lm.get("source_run_id") or source_run_id
    except Exception as e:
        print(f"note: logged model lookup failed: {str(e)[:120]}")

rel_path = None
for rid in dict.fromkeys([source_run_id, champ.run_id]):
    try:
        rel_path = mlflow.artifacts.download_artifacts(run_id=rid, artifact_path="relativities.csv")
        coef_run_id = rid
        break
    except Exception as e:
        print(f"note: no relativities.csv on run {rid}: {str(e)[:100]}")
if rel_path is None:
    raise RuntimeError("Champion coefficients (relativities.csv) not found; cannot compare like for like.")

rel = pd.read_csv(rel_path)
intercept = float(rel.loc[rel["feature"] == "intercept", "coefficient"].iloc[0])
champ_coef = rel[rel["feature"] != "intercept"].set_index("feature")["coefficient"]
missing_in_candidate = [f for f in champ_coef.index if f not in X_test.columns]
X_test_champ = X_test.reindex(columns=list(champ_coef.index), fill_value=0.0)
mu_champ = np.exp(intercept + X_test_champ.values @ champ_coef.values)
print(f"Champion v{champion_version}: {len(champ_coef)} coefficients from run {coef_run_id}"
      f"{'; ' + str(len(missing_in_candidate)) + ' encoded levels absent in candidate' if missing_in_candidate else ''}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Like-for-like comparison on identical holdout rows

# COMMAND ----------

def lorenz_gini(y_true, y_pred):
    """Same definition as the champion's logged `gini` metric in freq_glm.py."""
    order = np.argsort(-np.asarray(y_pred))
    yt = np.asarray(y_true)[order]
    cum_y = np.cumsum(yt) / (yt.sum() + 1e-9)
    cum_n = np.arange(1, len(yt) + 1) / len(yt)
    trapezoid = getattr(np, "trapezoid", None) or np.trapz   # numpy 2 renamed trapz
    return float(2 * trapezoid(cum_y, cum_n) - 1)


def poisson_deviance(y_true, mu):
    yt, mu = np.asarray(y_true, float), np.clip(np.asarray(mu, float), 1e-12, None)
    term = np.where(yt > 0, yt * np.log(yt / mu), 0.0)
    return float(2 * np.mean(term - (yt - mu)))


yt = y_test.values
m = {
    "champion_gini": lorenz_gini(yt, mu_champ), "challenger_gini": lorenz_gini(yt, mu_chal),
    "champion_deviance": poisson_deviance(yt, mu_champ), "challenger_deviance": poisson_deviance(yt, mu_chal),
    "actual_mean_annual_freq": float(yt.mean() / exposure_years),
    "champion_mean_annual_freq": float(mu_champ.mean() / exposure_years),
    "challenger_mean_annual_freq": float(mu_chal.mean() / exposure_years),
}
m["gini_delta"] = m["challenger_gini"] - m["champion_gini"]
m["deviance_change_pct"] = 100 * (m["challenger_deviance"] - m["champion_deviance"]) / m["champion_deviance"]
m["mean_freq_shift_pct"] = 100 * (m["challenger_mean_annual_freq"] / m["champion_mean_annual_freq"] - 1)
m["champion_ae"] = m["actual_mean_annual_freq"] / m["champion_mean_annual_freq"]      # actual / expected
m["challenger_ae"] = m["actual_mean_annual_freq"] / m["challenger_mean_annual_freq"]
for k, v in m.items():
    print(f"  {k:28s} {v: .4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Monitored assumption: Bühlmann–Straub credibility K on the candidate

# COMMAND ----------

cred = pd.DataFrame({"pc": pdf["postcode_sector"], "y": y}).dropna(subset=["pc"])
g = cred.groupby("pc")["y"]
n_i, xbar_i = g.size().astype(float), g.mean()
N, G = float(n_i.sum()), len(n_i)
xbar = float(cred["y"].mean())
s2 = float(((cred["y"] - g.transform("mean")) ** 2).sum() / (N - G))                     # within-postcode (EPV)
a = float(((n_i * (xbar_i - xbar) ** 2).sum() - (G - 1) * s2) / (N - (n_i ** 2).sum() / N))  # between (VHM)
k_est = s2 / a if a > 0 else float("inf")
z_gov, z_est = float((n_i / (n_i + governed_k)).mean()), float((n_i / (n_i + k_est)).mean()) if np.isfinite(k_est) else 0.0
k_ratio = k_est / governed_k if np.isfinite(k_est) else float("inf")
k_flag = "within tolerance" if 0.5 <= k_ratio <= 2.0 else "outside 0.5x-2x of governed K: review the credibility assumption"
print(f"Credibility K: governed {governed_k:.0f} vs data-estimated {k_est:,.1f} ({k_flag}); "
      f"mean Z {z_gov:.3f} vs {z_est:.3f} over {G} postcodes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Recommendation (evidence for the human, not a decision)

# COMMAND ----------

better_rank = m["gini_delta"] >= 0.005
better_fit = m["deviance_change_pct"] <= -0.5
if better_rank or better_fit:
    recommendation = "promote"
    rationale = (f"On the drifted book the challenger {'ranks risk better' if better_rank else 'fits better'}: "
                 f"Gini {m['champion_gini']:.4f} → {m['challenger_gini']:.4f} (Δ {m['gini_delta']:+.4f}), "
                 f"Poisson deviance {m['deviance_change_pct']:+.2f}%, mean annual frequency {m['mean_freq_shift_pct']:+.2f}% "
                 f"(A/E champion {m['champion_ae']:.3f}, challenger {m['challenger_ae']:.3f}).")
else:
    recommendation = "hold"
    rationale = (f"The champion still holds on the drifted book: Gini Δ {m['gini_delta']:+.4f}, "
                 f"deviance {m['deviance_change_pct']:+.2f}%. Keep monitoring.")
print(f"Recommendation: {recommendation.upper()} — {rationale}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Log + register the challenger (separate UC model; champion untouched)

# COMMAND ----------

class DriftChallengerGLM(mlflow.pyfunc.PythonModel):
    """Poisson GLM (log link) as coefficients only: replays the champion's encoding at predict time."""
    def __init__(self, intercept, coefs, feature_names, raw_features):
        self.intercept, self.coefs = float(intercept), np.asarray(coefs, float)
        self.feature_names, self.raw_features = list(feature_names), list(raw_features)

    def predict(self, context, model_input, params=None):
        df = pd.DataFrame({c: model_input[c] if c in model_input.columns else 0.0 for c in self.raw_features})
        for c in df.columns:
            if df[c].dtype == "object":
                df[c] = df[c].astype(str).where(df[c].notna(), "(null)")
            else:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).astype(float)
        Xd = pd.get_dummies(df, drop_first=True, dtype=float).reindex(columns=self.feature_names, fill_value=0.0)
        return np.exp(self.intercept + Xd.fillna(0.0).values @ self.coefs)


mlflow.set_experiment(experiment_path)
retrain_id = str(uuid.uuid4())
model_obj = DriftChallengerGLM(res.params[0], res.params[1:], feature_names, FEATURES)
sample = raw_X.head(5)
signature = infer_signature(sample, model_obj.predict(None, sample))
with mlflow.start_run(run_name=f"{model_name}_drift_challenger_{check_run_id[:8]}") as run:
    mlflow.set_tags({"trigger": "drift", "drift_check_run_id": check_run_id, "champion_model": f"{fqn}.{model_name}",
                     "champion_version": champion_version, "candidate_table": cand_table,
                     "candidate_delta_version": str(candidate_version), "promotion": "pending_human_review"})
    mlflow.log_params({"family": "Poisson", "link": "log", "target": TARGET, "raw_features": len(FEATURES),
                       "encoded_features": len(feature_names), "train_rows": int(train.sum()),
                       "test_rows": int((~train).sum()), "split": "crc32(policy_id) % 100 < 80",
                       "max_psi": max_psi, "drifted_features": drifted_features[:500] or "(none)",
                       "governed_credibility_k": governed_k, "champion_coefficients_run": coef_run_id})
    mlflow.log_metrics({**{k: float(v) for k, v in m.items()},
                        "credibility_k_estimated": float(k_est) if np.isfinite(k_est) else -1.0,
                        "credibility_mean_z_governed": z_gov, "credibility_mean_z_estimated": z_est})
    tmp = tempfile.mkdtemp()
    pd.DataFrame({"feature": ["intercept"] + feature_names, "coefficient": res.params.tolist(),
                  "relativity": np.exp(res.params).tolist(), "p_value": res.pvalues.tolist()}
                 ).to_csv(os.path.join(tmp, "relativities.csv"), index=False)
    with open(os.path.join(tmp, "champion_vs_challenger.json"), "w") as fh:
        json.dump({"metrics": m, "recommendation": recommendation, "rationale": rationale,
                   "credibility": {"governed_k": governed_k, "estimated_k": k_est, "flag": k_flag}}, fh, indent=2, default=str)
    mlflow.log_artifacts(tmp)
    info = mlflow.pyfunc.log_model(artifact_path="model", python_model=model_obj, signature=signature,
                                   input_example=sample, pip_requirements=["numpy", "pandas"],
                                   registered_model_name=f"{fqn}.{challenger_name}")
    mlflow_run_id = run.info.run_id

full_chal = f"{fqn}.{challenger_name}"
chal_version = str(info.registered_model_version) if getattr(info, "registered_model_version", None) else \
    str(max(int(v.version) for v in client.search_model_versions(f"name='{full_chal}'")))
for k, v in {"trigger": "drift", "drift_check_run_id": check_run_id, "max_psi": max_psi,
             "drifted_features": (drifted_features or "(none)")[:250], "champion_version": champion_version,
             "candidate_delta_version": str(candidate_version), "recommendation": recommendation,
             "promotion": "pending_human_review"}.items():
    client.set_model_version_tag(full_chal, chal_version, k, str(v))
client.set_registered_model_alias(full_chal, "challenger", chal_version)
try:
    client.update_registered_model(full_chal, description=(
        f"Drift-triggered challenger for {fqn}.{model_name}. Retrained automatically when new data drifts the book; "
        "never served automatically. Promotion to champion is a named human decision recorded in model_promotion_requests."))
except Exception as e:
    print(f"description: {e}")
print(f"Registered {full_chal} v{chal_version} @challenger (run {mlflow_run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Evidence tables + promotion request + audit

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fqn}.model_retrain_log (
  retrain_id STRING, check_run_id STRING, triggered_at TIMESTAMP, trigger STRING,
  model_name STRING, champion_version STRING, challenger_model STRING, challenger_version STRING,
  mlflow_run_id STRING, max_psi STRING, drifted_features STRING,
  candidate_table STRING, candidate_delta_version LONG, train_rows LONG, test_rows LONG, split_method STRING,
  champion_gini DOUBLE, challenger_gini DOUBLE, gini_delta DOUBLE,
  champion_deviance DOUBLE, challenger_deviance DOUBLE, deviance_change_pct DOUBLE,
  actual_mean_annual_freq DOUBLE, champion_mean_annual_freq DOUBLE, challenger_mean_annual_freq DOUBLE,
  mean_freq_shift_pct DOUBLE, credibility_k_governed DOUBLE, credibility_k_estimated DOUBLE,
  credibility_flag STRING, recommendation STRING, rationale STRING)
COMMENT 'Every drift-triggered challenger retrain: trigger, champion vs challenger on the same holdout, credibility check, recommendation.'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fqn}.model_promotion_requests (
  request_id STRING, created_at TIMESTAMP, model_name STRING, champion_version STRING,
  challenger_model STRING, challenger_version STRING, trigger STRING, evidence_retrain_id STRING,
  recommendation STRING, rationale STRING, status STRING,
  decided_by STRING, decided_at TIMESTAMP, decision_notes STRING)
COMMENT 'Champion promotion requests raised by automated retrains. Status stays pending_human_review until a named human decides.'
""")

log_schema = StructType([StructField(n, t) for n, t in [
    ("retrain_id", StringType()), ("check_run_id", StringType()), ("triggered_at", TimestampType()), ("trigger", StringType()),
    ("model_name", StringType()), ("champion_version", StringType()), ("challenger_model", StringType()),
    ("challenger_version", StringType()), ("mlflow_run_id", StringType()), ("max_psi", StringType()),
    ("drifted_features", StringType()), ("candidate_table", StringType()), ("candidate_delta_version", LongType()),
    ("train_rows", LongType()), ("test_rows", LongType()), ("split_method", StringType()),
    ("champion_gini", DoubleType()), ("challenger_gini", DoubleType()), ("gini_delta", DoubleType()),
    ("champion_deviance", DoubleType()), ("challenger_deviance", DoubleType()), ("deviance_change_pct", DoubleType()),
    ("actual_mean_annual_freq", DoubleType()), ("champion_mean_annual_freq", DoubleType()),
    ("challenger_mean_annual_freq", DoubleType()), ("mean_freq_shift_pct", DoubleType()),
    ("credibility_k_governed", DoubleType()), ("credibility_k_estimated", DoubleType()),
    ("credibility_flag", StringType()), ("recommendation", StringType()), ("rationale", StringType())]])
spark.createDataFrame([(
    retrain_id, check_run_id, now, "drift", f"{fqn}.{model_name}", champion_version, full_chal, chal_version,
    mlflow_run_id, max_psi, drifted_features, cand_table, candidate_version, int(train.sum()), int((~train).sum()),
    "crc32(policy_id) % 100 < 80", m["champion_gini"], m["challenger_gini"], m["gini_delta"],
    m["champion_deviance"], m["challenger_deviance"], m["deviance_change_pct"],
    m["actual_mean_annual_freq"], m["champion_mean_annual_freq"], m["challenger_mean_annual_freq"],
    m["mean_freq_shift_pct"], governed_k, float(k_est) if np.isfinite(k_est) else None, k_flag,
    recommendation, rationale)], log_schema).write.mode("append").saveAsTable(f"{fqn}.model_retrain_log")

req_schema = StructType([StructField(n, t) for n, t in [
    ("request_id", StringType()), ("created_at", TimestampType()), ("model_name", StringType()),
    ("champion_version", StringType()), ("challenger_model", StringType()), ("challenger_version", StringType()),
    ("trigger", StringType()), ("evidence_retrain_id", StringType()), ("recommendation", StringType()),
    ("rationale", StringType()), ("status", StringType()), ("decided_by", StringType()),
    ("decided_at", TimestampType()), ("decision_notes", StringType())]])
request_id = str(uuid.uuid4())
spark.createDataFrame([(
    request_id, now, f"{fqn}.{model_name}", champion_version, full_chal, chal_version, "drift", retrain_id,
    recommendation, rationale, "pending_human_review", None, None, None)], req_schema
).write.mode("append").saveAsTable(f"{fqn}.model_promotion_requests")

summary = {"retrain_id": retrain_id, "promotion_request_id": request_id, "check_run_id": check_run_id,
           "challenger": f"{full_chal} v{chal_version}", "champion": f"{fqn}.{model_name} v{champion_version}",
           "metrics": {k: round(v, 4) for k, v in m.items()},
           "credibility_k": {"governed": governed_k, "estimated": round(k_est, 1) if np.isfinite(k_est) else None, "flag": k_flag},
           "recommendation": recommendation, "status": "pending_human_review"}
try:
    det = json.dumps(summary, default=str).replace("'", "''")
    spark.sql(f"""
        INSERT INTO {fqn}.audit_log
          (event_id, event_type, entity_type, entity_id, entity_version, user_id, timestamp, details, source)
        SELECT uuid(), 'drift_retrain_triggered', 'model', '{model_name}', '{chal_version}', '{user}',
               current_timestamp(), '{det}', 'job'
    """)
except Exception as e:
    print(f"audit: {e}")

print(json.dumps(summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(summary, default=str))
