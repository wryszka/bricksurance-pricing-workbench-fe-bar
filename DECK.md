# Bricksurance Pricing Workbench — business deck

*Audience: Head of Pricing (executive sponsor) and the pricing actuarial team (domain owner) at a UK motor insurer. "Demonstrated" figures come from the live prototype on a synthetic 50,000-policy motor book (plus real public UK postcode data); they prove the mechanism, not a customer's uplift. "Sizing" figures are scenarios for the insurer's own inputs.*

---

## Slide 1: Price like the market leaders, on one governed platform

**Better calculations, fresher data, more live features, full governance and fewer licences, all in one place.**

- UK motor is won on aggregators by the most accurate price; the analytical leaders (Admiral, Hastings) set the pace
- Slower or blunter pricers get adversely selected: they win the risks others priced as worse
- This prototype runs the whole pricing chain on Databricks, from raw data to an approved live price

---

## Slide 2: Why pricing falls behind

| | Executive sponsor (Head of Pricing / CUO) | Domain owner (pricing actuaries) |
|---|---|---|
| **Pain** | Margin lost to adverse selection; rising FCA scrutiny (GIPP, Consumer Duty fair value); a stack of separate pricing licences | Data, models, optimisation, rating engine and monitoring in separate tools joined by exports; slow route from a new factor to a live price |
| **Measured on** | Loss ratio, expected profit, retention, tool cost, regulatory findings | Model lift (Gini), time to deploy a rate change, audit questions answered |

---

## Slide 3: What we built — one loop, one platform

**Raw data** → **Lakeflow** (ingest + 14 quality rules) → **Unity Catalog** (governed, lineage, audit, **metric views** as the one definition of every KPI) → **Lakebase** (live features) → **ML** (frequency, severity, demand, fraud, elasticity + optimiser) → **Agent Bricks** (Supervisor front door + governance Knowledge Assistant, over Agent Framework agents) → **Genie** (plain-English questions over the metric views) → **Databricks App** (review, approve, deploy)

The same governed data flows through every step, with no copies and no hand-offs.

---

## Slide 4: Value 1 — better calculations

- Real external data lifts model **Gini 0.11 → 0.25**
- The demand model **recovers the true price sensitivity** in the test data (−6.0 true vs −5.6 recovered)
- The optimiser finds **+9.2% expected profit** inside ±15% of the break-even price (demonstrated, synthetic book)

**Sizing:** each 1 point of loss ratio on a £500m book = **£5m a year**. A pilot on your data measures your real gain.

---

## Slide 5: Value 2 — much fresher data

- New data passes quality rules and reaches models and live prices in **one governed pipeline**
- The whole pricing estate rebuilds serverless in **~30–40 minutes**
- An approved rate change deploys from the app; no re-coding into another system

**Sizing:** every month a price lags the market is a month of adverse selection on aggregators.

---

## Slide 6: Value 3 — more features, live at quote time

- **43 features per motor policy** (104 commercial) served live from Lakebase
- A full price from just a policy number in **~240 ms**, with four models scored in one call
- Real postcode, deprivation and telematics signals usable *at the quote*, not only offline

**Sizing:** every predictive factor your rating engine can't carry is lift left on the table.

---

## Slide 7: Value 4 — full governance

- Lineage from source data to model to price, plus an immutable audit log
- No price goes live without a **named human approval**; the corridor is re-checked at deploy
- A decision record per change: models, versions, data snapshot, approver, fair-value result
- A GIPP-enforced renewal solver; when a plan fails the fair-value screen, the AI recommender advises **REVISE BEFORE DEPLOY**

**Sizing:** audit and remediation effort per review cycle replaced by one query; lower GIPP / Consumer Duty risk.

---

## Slide 8: Value 5 — lower licensing cost

- Data prep, modelling, optimisation, live serving, BI and the app on **one pay-per-use, serverless platform**
- Runs alongside the existing rating engine during migration, so tools retire one at a time

**Sizing:** today's separate licences minus Databricks consumption — typically a seven-figure annual line for a mid-size insurer.

---

## Slide 9: The value on one page

| Value | Demonstrated in the prototype | How to size it for your book |
|---|---|---|
| Better calculations | Gini 0.11 → 0.25; true price sensitivity recovered; +9.2% expected profit (synthetic) | 1 loss-ratio point on £500m = £5m/yr |
| Fresher data | One pipeline from data to live price; ~30–40 min full rebuild | Months of price lag × adverse selection |
| More live features | 43 motor features per quote, ~240 ms | Lift from factors you can't use today |
| Full governance | Lineage, audit, human gate, decision records, fair-value screen, GIPP | Audit hours saved, regulatory risk avoided |
| Lower licensing | One consumption platform | Current licences minus consumption |

---

## Slide 10: Why Databricks, and the next step

- **One platform** for data, models, serving, agents, BI and the app, so lineage is unbroken
- **Open and deployable as code**; works alongside existing rating engines
- **Part of Bricksurance**, an open-source insurance operating system on Databricks covering pricing, underwriting, claims, reserving, reinsurance, IFRS 17 and Solvency II

**Next step:** a pilot on one motor segment with your data, measuring Gini lift, profit uplift inside your corridor, time from data to live price, and the licence cost you could retire.
