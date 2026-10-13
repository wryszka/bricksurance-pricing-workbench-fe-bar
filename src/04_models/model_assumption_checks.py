# Databricks notebook source
# MAGIC %md
# MAGIC # Model assumption checks — every modelling trade-off, measured
# MAGIC
# MAGIC The workbench is a **framework for running an insurer's own pricing models**; the GLMs
# MAGIC here are reference models on a synthetic book. This notebook is the reusable pattern
# MAGIC for model governance: each assumption baked into a registered model gets a
# MAGIC reproducible sensitivity run, logged to MLflow and written to Unity Catalog, so a
# MAGIC reviewer can see what each choice costs instead of taking it on trust.
# MAGIC
# MAGIC | # | Assumption in the reference models | Check |
# MAGIC |---|---|---|
# MAGIC | 1 | One 80/20 split is representative | Gini of baseline vs full real-data model over 20 re-salted splits |
# MAGIC | 2 | `neighbourhood_claim_frequency` doesn't leak the target | Rebuild the factor from training-fold claims only (out-of-fold) and compare test Gini |
# MAGIC | 3 | Credibility constant K = 100 | Data-estimated Bühlmann–Straub K, and test Gini at K = 0 … 10,000 |
# MAGIC | 4 | Log link for the Gamma severity GLM | Log vs canonical (inverse) link: fit, AIC, invalid predictions |
# MAGIC | 5 | No exposure offset (constant 5-year window) | Refit with `offset = log(5)`: coefficients and annual predictions |
# MAGIC
# MAGIC **Output:** `model_assumption_checks` (one row per check metric, with a verdict),
# MAGIC one MLflow run, one `audit_log` event.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_pricing_v2_aws_us_catalog")
dbutils.widgets.text("schema_name", "pricing_workbench_gen2")
dbutils.widgets.text("n_resplits", "20")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
fqn = f"{catalog}.{schema}"
N_RESPLITS = int(dbutils.widgets.get("n_resplits"))

# COMMAND ----------

import json, zlib
import mlflow
import numpy as np
import pandas as pd
import statsmodels.api as sm
from datetime import datetime, timezone

try:
    mlflow.set_experiment("/Workspace/Shared/.bundle/pricing-workbench-gen2/experiments/model_assumption_checks")
except Exception:
    pass

# Same feature sets as challenger_comparison.py, so the checks test the published ablation.
BASELINE_FEATURES = [
    "annual_turnover", "sum_insured", "building_age_years", "current_premium",
    "flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index", "subsidence_risk",
    "composite_location_risk", "credit_score", "ccj_count", "years_trading",
    "business_stability_score", "market_median_rate",
    "credit_default_probability", "director_stability_score",
    "employee_count_est", "distance_to_coast_km", "population_density_per_km2",
    "elevation_metres", "annual_rainfall_mm",
]
REAL_FACTORS = ["urban_score", "is_coastal", "deprivation_composite", "neighbourhood_claim_frequency"]
NCF = "neighbourhood_claim_frequency"
K_GOVERNED = 100          # the value derive_factors.py uses
EXPOSURE_YEARS = 5.0      # claim_count_5y covers a 5-year window for every policy

upt_name = f"{fqn}.unified_pricing_table_live"
upt_delta_version = spark.sql(f"DESCRIBE HISTORY {upt_name} LIMIT 1").collect()[0]["version"]
pdf = (spark.table(upt_name)
       .select("policy_id", "postcode_sector", "claim_count_5y", "total_incurred_5y",
               *BASELINE_FEATURES, *REAL_FACTORS)
       .toPandas())
pdf["y"] = pd.to_numeric(pdf["claim_count_5y"], errors="coerce").fillna(0).astype(float)
for c in BASELINE_FEATURES + REAL_FACTORS + ["total_incurred_5y"]:
    pdf[c] = pd.to_numeric(pdf[c], errors="coerce").fillna(0).astype(float)
print(f"{len(pdf):,} policies · UPT Delta version {upt_delta_version}")

# COMMAND ----------

def gini(y_true, y_pred):
    """Normalised Gini, identical to challenger_comparison.py."""
    arr = np.asarray(list(zip(y_true, y_pred, np.arange(len(y_true)))), dtype=float)
    arr = arr[np.lexsort((arr[:, 2], -arr[:, 1]))]
    total = arr[:, 0].sum()
    if total == 0:
        return 0.0
    raw = (np.cumsum(arr[:, 0]) / total).sum() - (len(arr) + 1) / 2.0
    perf_arr = np.sort(y_true)[::-1]
    perf = (np.cumsum(perf_arr) / total).sum() - (len(perf_arr) + 1) / 2.0
    return float(raw / perf) if perf else 0.0


def split_mask(salt=""):
    """Stable 80/20 split (zlib.crc32 is deterministic across processes)."""
    return pdf["policy_id"].map(lambda x: zlib.crc32(f"{salt}{x}".encode()) % 100 < 80).values


def poisson_gini(train, test, features, offset_train=None):
    fit = sm.GLM(train["y"].values, sm.add_constant(train[features].values, has_constant="add"),
                 family=sm.families.Poisson(link=sm.families.links.Log()),
                 offset=offset_train).fit()
    pred = fit.predict(sm.add_constant(test[features].values, has_constant="add"))
    return gini(test["y"].values, pred), fit


def credibility_factor(train, apply_to, k):
    """neighbourhood_claim_frequency rebuilt from TRAINING-fold claims only."""
    overall = train["y"].mean()
    g = train.groupby("postcode_sector")["y"].agg(["count", "mean"])
    z = g["count"] / (g["count"] + k) if np.isfinite(k) else 0.0 * g["count"]
    fac = (z * g["mean"] + (1 - z) * overall).rename("f")
    return apply_to["postcode_sector"].map(fac).fillna(overall).values


rows = []
def record(check_id, check, metric, value, detail="", verdict=""):
    rows.append((check_id, check, metric, None if value is None else float(value), detail, verdict))
    print(f"[{check_id}] {metric:42s} {'' if value is None else f'{value:.4f}'}  {verdict}")

FULL = BASELINE_FEATURES + REAL_FACTORS
m0 = split_mask()                      # the canonical split (same as challenger_comparison.py)
train0, test0 = pdf[m0], pdf[~m0]

# COMMAND ----------

# MAGIC %md ## 1 · Is one split representative? (re-salted splits)

# COMMAND ----------

base_g, full_g = [], []
for i in range(N_RESPLITS):
    m = split_mask(salt=f"resplit{i}:")
    tr, te = pdf[m], pdf[~m]
    base_g.append(poisson_gini(tr, te, BASELINE_FEATURES)[0])
    full_g.append(poisson_gini(tr, te, FULL)[0])
lift = np.array(full_g) - np.array(base_g)
record(1, "split_stability", "baseline_gini_mean", np.mean(base_g), f"sd {np.std(base_g):.4f} over {N_RESPLITS} splits")
record(1, "split_stability", "full_gini_mean", np.mean(full_g), f"sd {np.std(full_g):.4f} over {N_RESPLITS} splits")
record(1, "split_stability", "lift_mean", lift.mean(),
       f"sd {lift.std():.4f}, min {lift.min():.4f}, max {lift.max():.4f}",
       "STABLE" if lift.min() > 0 else "UNSTABLE: lift not positive on every split")

# COMMAND ----------

# MAGIC %md ## 2 · Leakage guard: in-sample vs out-of-fold neighbourhood factor

# COMMAND ----------

g_in, _ = poisson_gini(train0, test0, FULL)          # factor as built (uses every policy's claims)
tr_oof, te_oof = train0.copy(), test0.copy()
tr_oof[NCF] = credibility_factor(train0, train0, K_GOVERNED)
te_oof[NCF] = credibility_factor(train0, test0, K_GOVERNED)
g_oof, _ = poisson_gini(tr_oof, te_oof, FULL)
n_pc = train0.groupby("postcode_sector").size()
self_weight = float((1.0 / (n_pc + K_GOVERNED)).mean())
record(2, "leakage_guard", "test_gini_in_sample_factor", g_in, "factor built from all policies' claims")
record(2, "leakage_guard", "test_gini_out_of_fold_factor", g_oof, "factor built from training-fold claims only")
record(2, "leakage_guard", "gini_inflation_from_leakage", g_in - g_oof,
       f"mean weight of a policy's own claims in its factor = {self_weight:.5f}",
       "NEGLIGIBLE" if abs(g_in - g_oof) < 0.005 else "MATERIAL: publish the out-of-fold factor")

# COMMAND ----------

# MAGIC %md ## 3 · Credibility constant K: data-estimated value and sensitivity

# COMMAND ----------

# Bühlmann–Straub on the training fold, one observation per policy.
grp = train0.groupby("postcode_sector")["y"]
n_i, mean_i = grp.count(), grp.mean()
N, G = n_i.sum(), len(n_i)
grand = (n_i * mean_i).sum() / N
epv = ((train0["y"] - train0["postcode_sector"].map(mean_i)) ** 2).sum() / (N - G)
vhm = ((n_i * (mean_i - grand) ** 2).sum() - (G - 1) * epv) / (N - (n_i ** 2).sum() / N)
k_hat = epv / vhm if vhm > 0 else float("inf")
record(3, "credibility_k", "k_estimated_buhlmann_straub", k_hat if np.isfinite(k_hat) else None,
       f"EPV {epv:.4f}, VHM {vhm:.6f}, {G} postcodes, median {int(n_i.median())} policies each",
       "NO CREDIBLE POSTCODE SIGNAL: VHM ≤ 0, credibility theory gives Z = 0" if not np.isfinite(k_hat)
       else ("CONSISTENT with K = 100" if 0.5 <= k_hat / K_GOVERNED <= 2 else "DIVERGES from K = 100"))
record(3, "credibility_k", "k_governed", K_GOVERNED, "value used in derive_factors.py")
for k in [0, 10, K_GOVERNED, 1000, 10000] + ([round(k_hat)] if np.isfinite(k_hat) and k_hat > 0 else []):
    tr_k, te_k = train0.copy(), test0.copy()
    tr_k[NCF] = credibility_factor(train0, train0, k)
    te_k[NCF] = credibility_factor(train0, test0, k)
    record(3, "credibility_k", f"test_gini_at_k_{int(k)}", poisson_gini(tr_k, te_k, FULL)[0],
           "out-of-fold factor")
z_med = float(n_i.median() / (n_i.median() + K_GOVERNED))
record(3, "credibility_k", "credibility_z_median_postcode", z_med,
       "Z = n / (n + K) at K = 100", "K barely binds at this book size" if z_med > 0.9 else "")

# COMMAND ----------

# MAGIC %md ## 4 · Link function for the Gamma severity GLM: log vs canonical (inverse)

# COMMAND ----------

claimants = pdf[(pdf["y"] > 0) & (pdf["total_incurred_5y"] > 0)].copy()
claimants["sev"] = claimants["total_incurred_5y"] / claimants["y"]
mc = split_mask()[(pdf["y"] > 0).values & (pdf["total_incurred_5y"] > 0).values]
X = claimants[BASELINE_FEATURES]
X = (X - X.mean()) / X.std().replace(0, 1)            # same scaling for both links
Xtr, Xte = sm.add_constant(X[mc].values, has_constant="add"), sm.add_constant(X[~mc].values, has_constant="add")
ytr, yte = claimants["sev"].values[mc], claimants["sev"].values[~mc]
for link_name, link in [("log", sm.families.links.Log()), ("inverse_canonical", sm.families.links.InversePower())]:
    try:
        start = np.zeros(Xtr.shape[1])
        start[0] = np.log(ytr.mean()) if link_name == "log" else 1.0 / ytr.mean()
        fit = sm.GLM(ytr, Xtr, family=sm.families.Gamma(link=link)).fit(start_params=start, maxiter=100)
        lin = Xte @ fit.params
        invalid = float(np.mean(lin <= 0)) if link_name.startswith("inverse") else 0.0
        pred = fit.predict(Xte)
        record(4, "link_function", f"severity_aic_{link_name}", fit.aic, f"converged={fit.converged}")
        record(4, "link_function", f"severity_test_gini_{link_name}", gini(yte, np.where(np.isfinite(pred), pred, 0)))
        record(4, "link_function", f"share_invalid_predictions_{link_name}", invalid,
               "linear predictor ≤ 0 means a negative or infinite mean claim cost")
    except Exception as e:
        record(4, "link_function", f"severity_fit_{link_name}", None, f"failed: {str(e)[:200]}", "FIT FAILED")
record(4, "link_function", "frequency_link", None, "Poisson log link is the canonical link: no trade-off", "CANONICAL")

# COMMAND ----------

# MAGIC %md ## 5 · Exposure: no offset vs `offset = log(5)`

# COMMAND ----------

_, fit_no = poisson_gini(train0, test0, FULL)
_, fit_off = poisson_gini(train0, test0, FULL, offset_train=np.full(len(train0), np.log(EXPOSURE_YEARS)))
Xte0 = sm.add_constant(test0[FULL].values, has_constant="add")
annual_no = fit_no.predict(Xte0) / EXPOSURE_YEARS                 # what the scorer does today
annual_off = fit_off.predict(Xte0)                                # offset model at 1 year of exposure
record(5, "exposure_offset", "max_abs_diff_non_intercept_coefs", np.max(np.abs(fit_no.params[1:] - fit_off.params[1:])))
record(5, "exposure_offset", "intercept_shift", fit_off.params[0] - fit_no.params[0], f"expected −log(5) = {-np.log(EXPOSURE_YEARS):.4f}")
record(5, "exposure_offset", "max_abs_diff_annual_frequency", np.max(np.abs(annual_no - annual_off)),
       "", "EQUIVALENT while exposure is constant; add the offset when policy terms vary")

# COMMAND ----------

# MAGIC %md ## Write results

# COMMAND ----------

now = datetime.now(timezone.utc).replace(tzinfo=None)
out = spark.createDataFrame(
    [(*r, int(upt_delta_version), now) for r in rows],
    "check_id INT, check_name STRING, metric STRING, value DOUBLE, detail STRING, verdict STRING, upt_delta_version BIGINT, computed_at TIMESTAMP")
out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.model_assumption_checks")
spark.sql(f"COMMENT ON TABLE {fqn}.model_assumption_checks IS 'Measured cost of each modelling assumption in the reference pricing GLMs (split stability, leakage, credibility K, link function, exposure offset)'")

with mlflow.start_run(run_name="model_assumption_checks") as run:
    mlflow.log_param("upt_delta_version", upt_delta_version)
    mlflow.log_param("n_resplits", N_RESPLITS)
    for r in rows:
        if r[3] is not None and np.isfinite(r[3]):
            mlflow.log_metric(f"c{r[0]}_{r[2]}"[:250], r[3])

try:
    user = spark.sql("SELECT current_user()").collect()[0][0]
    det = json.dumps({"mlflow_run_id": run.info.run_id, "checks": len(rows),
                      "verdicts": sorted({r[5] for r in rows if r[5]})}).replace("'", "''")
    spark.sql(f"""
        INSERT INTO {fqn}.audit_log
          (event_id, event_type, entity_type, entity_id, entity_version, user_id, timestamp, details, source)
        SELECT uuid(), 'model_assumption_checks', 'model', 'freq_glm', '{upt_delta_version}', '{user}',
               current_timestamp(), '{det}', 'notebook'
    """)
except Exception as e:
    print(f"audit: {e}")

display(out)
