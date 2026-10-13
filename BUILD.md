# BUILD.md: how this was built with AI

This workbench was built by one Solutions Architect, with AI as a force multiplier at every stage. **103 of the 104 commits** in this repo are co-authored with an AI coding agent. The architecture, the decisions and the review were human.

## Toolchain

| Tool | Used for |
|---|---|
| **Claude Code** (Anthropic Claude, CLI agent) | Writing and refactoring the notebooks, Declarative Automation Bundle resources, the FastAPI backend, the React frontend and the agents; running deploy → verify loops against the live workspace |
| **Databricks CLI + Declarative Automation Bundles** | Every job, pipeline, endpoint and the app are defined as code in `databricks.yml` + `resources/` and deployed with `databricks bundle deploy` |
| **Databricks Field Engineering AI plugins and skills** | Product-specific guidance for Apps, bundles, Lakebase, Genie spaces, model serving and Agent Framework |
| **Claude on the Databricks Foundation Model API** | The runtime intelligence inside the product: the chat agent personas, the governance agent and the committee-paper drafting |
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

## Where AI mattered most

- **Speed of breadth.** 67 jobs, a declarative pipeline, a semantic layer of 5 metric views, 15 registered models, 6 serving endpoints, 2 Lakebase online stores, 2 Genie spaces, an Agent Bricks Supervisor and Knowledge Assistant, an MCP server and a multi-page app. One person built all of it, part-time, with AI.
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
