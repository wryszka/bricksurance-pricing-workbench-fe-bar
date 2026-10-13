# BUILD.md: how this was built with AI

This workbench was built by one Solutions Architect, with AI as a force multiplier at every stage. **103 of the 104 commits** in this repo are co-authored with an AI coding agent. The architecture, the decisions and the review were human.

## Toolchain

| Tool | Used for |
|---|---|
| **Claude Code** (Anthropic Claude, CLI agent) | Writing and refactoring the notebooks, Declarative Automation Bundle resources, the FastAPI backend, the React frontend and the agents; running deploy → verify loops against the live workspace |
| **Databricks CLI + Declarative Automation Bundles** | Every job, pipeline, endpoint and the app are defined as code in `databricks.yml` + `resources/` and deployed with `databricks bundle deploy` |
| **Databricks Field Engineering AI plugins and skills** | Product-specific guidance for Apps, bundles, Lakebase, Genie spaces, model serving and Agent Framework |
| **Claude on the Databricks Foundation Model API** | The runtime intelligence inside the product: the chat agent personas, the governance agent and the committee-paper drafting |
| **MLflow GenAI evaluation (LLM judges)** | AI checking AI: the governance Knowledge Assistant is scored against ground truth read from Unity Catalog and MLflow, with the same LLM minus the documents as a baseline ([evidence](evidence/EXECUTION_OUTPUT.md)) |
| **Multi-agent review panel** | Independent AI reviewers with distinct roles (senior developer, security, actuary, product, incumbent champion…) scoring the build against a written standard. Reports are in [`docs/REVIEW/`](docs/REVIEW/) |

## Workflow

1. **Specification first.** Each capability starts as a written spec (for example [`docs/optimisation_demo_spec.md`](docs/optimisation_demo_spec.md)). Where the spec and the build later diverged, that is recorded rather than glossed over in [`docs/OPTIMIZATION_RECONCILIATION.md`](docs/OPTIMIZATION_RECONCILIATION.md).
2. **AI writes, the live platform verifies.** The agent writes code, deploys it with bundles, runs the job, then queries the resulting Unity Catalog tables to check the numbers. Nothing is marked done until it has run on the real workspace. That loop produced the evidence in [`evidence/RUN_EVIDENCE.md`](evidence/RUN_EVIDENCE.md).
3. **Decisions are logged.** Every non-trivial choice is written up with a verification note in [`docs/DECISIONS.md`](docs/DECISIONS.md).
4. **Adversarial AI review before promotion.** A six- and then seven-agent panel reviews each phase. Blockers are fixed before shipping. One example: the panel found that the MCP deploy tool bypassed the admin gate that the app enforces. That was fixed so both entry points share one gate and write the same immutable decision record.

## Prompt patterns (real, from the build session)

The build was driven as a series of **numbered component prompts** with a fixed shape:

1. **Context:** what already exists, referencing earlier prompts.
2. **Audience and constraints:** for example "this is a public demo", "serverless only".
3. **IMPORTANT:** guard-rails on what *not* to build.
4. **YOUR TASK:** numbered files, each with acceptance checks.

A verbatim excerpt:

```text
Context: Models were trained using FeatureLookup and logged with
FeatureEngineeringClient.log_model() (Prompt 2). The UPT is published to a
Databricks Online Feature Store via Lakebase (Prompt 7). This means model
serving endpoints will AUTOMATICALLY look up features — this is the payoff.

This is a public demo. The "send a policy_id, get a price back in <100ms" moment
is the key technical wow factor.

IMPORTANT: Because models were logged with fe.log_model() and FeatureLookup
specs, Mosaic AI Model Serving automatically resolves features from the online
store. DO NOT build custom feature-fetching code in the endpoint. The platform
does this natively — and showing that is the whole point.

YOUR TASK: Deploy models to serving endpoints with automatic feature lookup.
1. Create src/06_serving/deploy_model_endpoint.py ... champion/challenger 90/10 ...
2. Create src/06_serving/test_endpoint.py ... single request (just policy_id) →
   response + latency; 100 concurrent requests → p50/p95/p99 ...
3. Create src/06_serving/promote_model.py ... logs the promotion to audit_log
```

That `policy_id → price` call is exactly what the evidence shows today: `pwg2_motor_scorer` returns a full price in ~240 ms ([evidence](evidence/EXECUTION_OUTPUT.md)).

Other patterns that worked:
- **Correcting the platform choice.** *"this is a serverless only workspace … serverless v4 and newer now support ml libraries … I want the latest serverless version."* The agent had defaulted to classic ML compute, and the human steered it to the right platform capability.
- **Naming by plain-English intent.** *"unified because it unifies all product lines, all data ingestion and features in one place"*. This renamed a working title to the Unified Pricing Table.
- **Status checkpoints.** *"of the original plan — what have we built, and what still needs building?"* The agent keeps its plan honest against the repo.
- **Feature review, then build.** The semantic layer (UC metric views) and the Agent Bricks layer (Supervisor + governance Knowledge Assistant) were added in a final pass. The human set the scope ("use Agent Bricks for our agents, put the governance packs into a knowledge assistant, metric views for semantics"). The AI reviewed what already existed, built and deployed it, and verified each piece with live queries before writing the evidence.

**Build conversation ID (Claude Code):** `e652beac-9af1-4ef4-93e6-6c6331de3bc4`. That is the main build session, from the first version (30 Mar 2026) through gen2 (13 Sep 2026).

## Trade-offs in the code, measured

The workbench is a foundation for running an insurer's own models; the GLMs here are reference models on a synthetic book. Their design choices are still real trade-offs, and each one now has a measured cost in Unity Catalog (`model_assumption_checks`, `challenger_attribution_shapley`; job *Model evidence*; results in [EXECUTION_OUTPUT.md](evidence/EXECUTION_OUTPUT.md)).

| Trade-off | What we chose, and why | What it costs (measured live) | How it was decided |
|---|---|---|---|
| **Ablation order** | A sequential ladder (baseline → urban → coastal → deprivation → neighbourhood claim frequency) that reads like a story | Order-independent Shapley attribution over all 16 factor subsets agrees within 0.0003 Gini per factor; the order doesn't change the conclusion on this book | The human scoped it (*"let's add a3 and a4"*, 22 Apr 2026); the AI designed the ladder; the Shapley check was added in the review pass for this submission |
| **Claims-based factor and target leakage** | `neighbourhood_claim_frequency` is a credibility-weighted postcode claim rate: a real actuarial technique | Built from every policy's claims, it saw the holdout's own claims: test Gini 0.055 in-sample vs 0.043 out-of-fold, so about 0.013 of its lift was leakage. The published ablation now builds it out-of-fold, and its honest lift is +0.005 | Found by the AI-written assumption checks during the review pass. The human chose to publish the smaller, honest number |
| **Credibility constant K = 100** | A fixed, documented and editable parameter (Bühlmann *Z = n / (n + K)*) | Test Gini barely moves from K = 0 to K = 10,000 (0.043 → 0.041). The data-estimated between-postcode variance is ≤ 0, so on this synthetic book there is no credible postcode signal to weight. On a real book, K should be calibrated, and the drift job now logs the data-estimated K on every run | The AI proposed Bühlmann with K as a tuning parameter (derive_factors, Apr 2026); the human accepted it as illustrative |
| **Log link for Poisson and Gamma** | Multiplicative relativities, the form rating engines and actuaries use; predictions are always positive | Poisson log is canonical, so there's no trade-off. For Gamma severity, the canonical inverse link actually fits slightly better on this book (AIC 3.11m vs 3.15m, 1.3% lower, no invalid predictions), but its coefficients aren't multiplicative relativities and nothing stops it predicting a negative cost on new data. The log link gives up that fit for factors a rating engine can use. Neither ranks severity here (test Gini ≈ −0.04 for both): the synthetic commercial severity carries no signal | Standard practice, written into the notebook headers when the models were first generated (30 Mar 2026) |
| **No exposure offset** | `claim_count_5y` shares one 5-year window for every policy, so exposure is constant; the scorer divides by 5 | Refit with `offset = log(5)`: identical coefficients (max difference 0.0), intercept shifted by exactly −log 5, identical annual frequencies. Zero cost today; the offset is needed as soon as policy terms vary | Documented in `freq_glm.py`, with the exact change for variable terms |
| **A teaching notebook beside production** | `hello_world_freq.py` is a minimal, heavily commented GLM; it lists what it deliberately skips | Two notebooks to keep aligned, and the teaching one is never served | The human asked for four example notebooks (23 Apr 2026); the AI pushed back to one, so it wouldn't collide with the existing Model Factory tab; the human accepted |
| **Deterministic split** | An 80/20 split by `policy_id` hash, so runs are reproducible | Python's `hash()` is salted per process, so the "deterministic" split changed every run. Switched to `zlib.crc32`; over 20 re-salted splits the real-data lift is 0.018 ± 0.002 and positive on every split | Found by the AI in the review pass, fixed in place |
| **Reference models, not actuarial products** | Simple GLMs and GBMs keep every layer testable end to end | They are not models to price with; accuracy numbers show the mechanism | The human's call: Databricks provides the foundation; the insurer's actuaries own models and assumptions |
| **Drift retrains a challenger, never the champion** | Automatic retrain and evidence, human promotion | A drifted book is priced on the old champion until someone approves | Human-in-the-loop principle applied to model refresh |

## The review pass: AI auditing its own build

Before submission, the build was put through the same loop it uses on prices: the AI reviewed its own output against the live workspace, and every claim had to be re-proven there. What that pass found and fixed:

- **A headline that wasn't evidenced here.** Gini 0.11 → 0.25 came from an earlier workspace that no longer exists. The study was re-run live on this workspace and the headline corrected to **0.11 → 0.24**.
- **A leaky feature and a non-deterministic split** (above), both fixed and measured.
- **An overclaim.** The docs said the AI recommender *blocks* a plan that fails fair value. It *advises* REVISE BEFORE DEPLOY; the human gate is the block. The wording was corrected.
- **An agent misled by its tool.** The `explain_price` function labelled the corridor "±15% of the technical price", so the Supervisor described a +5% price as within ±15% of a figure it was 44% above. The function now names the break-even price explicitly.
- **A registry/document mismatch,** caught by the new LLM-judge evaluation. UC `@champion` is v36 for every model, but v36's governance pack calls it a "simulated historical replay", so the assistant (correctly following its documents) names a different version. This is logged as an open governance finding rather than hidden.
- **Salted hashes in other splits.** The same `hash()` pattern was in twelve modelling notebooks; ten now use `zlib.crc32`. The other two, production champions (`freq_glm.py`, `fraud_gbm.py`), also build the mask from a different frame than X and y (the key is excluded from the training set), so their holdout is random rather than keyed. They are left unchanged, because changing them writes new champion versions, and the fix is noted for their next retrain.
- **A drift gate that holds.** The injected vendor update is "stable" by the usual PSI bands (max 0.057), so at the standard 0.2 gate the drift job correctly does not retrain. The retrain path was exercised with a deliberately strict 0.05 gate: the challenger matched the champion on the same 99,000 rows (Gini 0.1199 vs 0.1203) and the recommendation was *hold*.

## Where AI mattered most

- **Speed of breadth.** 72 jobs, a declarative pipeline, a semantic layer of 6 metric views, 18 registered models, 6 serving endpoints, 2 Lakebase online stores, 2 Genie spaces, an Agent Bricks Supervisor and Knowledge Assistant, an MCP server and a multi-page app. One person built all of it, part-time, with AI.
- **Synthetic data with a real signal.** AI-written generators produce a 50,000-policy motor book and a 1.2M-quote stream with known, injected behaviour. That gives the elasticity and optimisation models something real to recover and the red-team checks something to test against.
- **Red-teaming its own output.** The review panel caught issues a solo builder misses: SQL injection paths, RBAC gaps on the MCP surface, and constraints that were advertised as enforced but were only monitored. The last of these was relabelled honestly rather than hidden.

## Where human judgement overruled the AI

- **Technical price definition.** Price enters the demand model as a ratio to the *loaded* (break-even) price, not the raw risk cost. Without this, real premiums read ~37% above "technical" and the ±15% corridor is meaningless.
- **Scope discipline.** A commercial-lines optimiser was proposed and **ruled out** (decision log, 2026-08-25). Motor proves the full loop, and a second demand-model concept would confuse the story.
- **Human in the loop by design.** The solver proposes and a named human approves. The corridor is re-checked server-side at deploy, and the AI agents can explain and draft but cannot deploy prices.
- **Gotchas caught in verification**, not in generated code. A stale premium field made the book look loss-making until it was switched to `loaded_premium`. MLflow `sklearn` flavour rejected LightGBM models, so they are logged with `mlflow.lightgbm`.

## Reproduce it

```bash
databricks bundle deploy -t <target>            # all jobs, pipeline, endpoints, app
databricks bundle run full_build -t <target>    # populate everything (serverless, ~30–40 min)
```

See [`docs/DEPLOY.md`](docs/DEPLOY.md) for the full fresh-workspace procedure.
