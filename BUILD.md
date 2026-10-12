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

## Where AI mattered most

- **Speed of breadth.** 65 jobs, a declarative pipeline, 15 registered models, 6 serving endpoints, 2 Lakebase online stores, 2 Genie spaces, an MCP server and a multi-page app, all in about four weeks of gen2 work (19 Aug – 13 Sep 2026), building on the earlier v1/v2 workbench.
- **Synthetic data with a real signal.** AI-written generators produce a 1M-policy motor book and quote stream with known, injected behaviour. That gives the elasticity and optimisation models something real to recover and the red-team checks something to test against.
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
