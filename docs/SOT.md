A) Executive Decision — Candidate system designs + scoring
Candidate designs (3–5) and what they optimize
| Design                                          | Core idea                                                                                                                | Feasible on your PC | Time-to-build | Data cost/need | Robustness | Maintainability | Edge potential | Operational risk |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ------------------: | ------------: | -------------: | ---------: | --------------: | -------------: | ---------------: |
| 1) Rules-only credit spreads                    | Sell put/call spreads based on IV rank + trend filters + fixed exits                                                     |               10/10 |          9/10 |           4/10 |       6/10 |            9/10 |           4/10 |             5/10 |
| 2) Rules + regime filter + volatility term/skew | Add regime detection + skew/term structure filters; still mostly rules                                                   |               10/10 |          7/10 |           6/10 |       7/10 |            8/10 |           5/10 |             5/10 |
| 3) **ML-ranked spread selection (selected)**    | Generate candidate verticals daily; ML predicts **net EV & tail risk**; hard risk gates; trade only top expected utility |                9/10 |          6/10 |           8/10 |       8/10 |            7/10 |           7/10 |             6/10 |
| 4) Deep learning sequence model                 | Transformer/LSTM predicts return distribution; then optimize spreads                                                     |                7/10 |          4/10 |           9/10 |       6/10 |            5/10 |           7/10 |             7/10 |
| 5) RL execution/strategy                        | RL chooses spread + manage intraday                                                                                      |                5/10 |          2/10 |          10/10 |       4/10 |            3/10 |           8/10 |             9/10 |

Selected Architecture: ML-ranked spread selection + hard risk rules + conservative execution

Why this wins under your constraints:

Vertical spreads are discrete instruments: you can treat “choose spread” as a ranking / contextual bandit problem (without full RL complexity).

You already have (or can get) NBBO historical via Databento OPRA, enabling realistic fill + spread-cost modeling. Databento OPRA provides consolidated trades and NBBO across US options venues.

Fees matter massively at $1k: ML’s job is not to predict direction; it’s to predict net-of-fees expected utility and trade rarely but better.

Keeps the system buildable locally, maintainable, and testable end-to-end.

B) System Architecture (Detailed SOT)
B1) Component diagram (text)

[CLI/UI]
→ [Orchestrator] (daily schedule + state machine)
→ [Data Services]

Databento Historical (OPRA + reference)

IBKR market data (live chain + quotes)
→ [Storage Layer]

Raw (DBN/Parquet) + Curated (Parquet) + Query (DuckDB)
→ [Feature & Candidate Builder]

chain snapshots, IV/skew/term, liquidity filters
→ [Model Service]

train, validate, calibrate, produce predictions + uncertainty
→ [Strategy Engine]

hard rules + optimization + “TRADE NOW / NO TRADE” decision
→ [Risk Engine]

exposure, drawdown, stress checks, kill-switch
→ [Execution Engine]

IBKR order builder (combo BAG + legs via conId) + retries + reconciliation
→ [Observability]

logs + metrics + alerts + post-trade analytics
→ [Audit / Reports]

immutable trade ledger + daily summary

B2) Data pipeline (sources, schemas, storage, refresh cadence)
Primary data sources

Historical options market data: Databento OPRA.PILLAR

OPRA disseminates top-of-book, last sale, and daily statistics for all US equity options exchanges, which Databento receives and distributes.

Databento OPRA dataset includes consolidated trades and NBBO across US options venues.

Databento expanded OPRA history by 10+ years (June 3, 2025 announcement).

Normalization detail: Databento transitioned from MBP-1/TBBO to CMBP‑1/TCBBO for consolidated top-of-book and consolidated best bid/offer.

Reference / metadata: Databento Reference API

Instrument definitions: point-in-time reference for symbol, expiration, strike, listing date, tick size, etc.

Symbology mapping: raw_symbol, instrument_id, parent, continuous.

Corporate actions / dividends: corporate actions and/or adjustment factors datasets (daily updates).

Live execution + live quotes: Interactive Brokers TWS API / IB Gateway

Contract creation best practice: use conId + exchange for stable identification.

Complex orders: spreads via BAG + ComboLeg with conIds.

Order pacing / OER risk exists; automated systems must avoid excessive order churn.

IBKR also introduced a Sync Wrapper (beta) for a more synchronous style; limited scope, recommended for paper testing.

Data capture strategy (practical + realistic)

You do not want to store every OPRA tick for every strike unless absolutely necessary. The SOT pipeline is two-tier:

Tier 1 — Chain snapshot dataset (what the models use)

Frequency: 1-minute bars during RTH (or selected windows like 11:30–12:00 ET and 15:30–15:59 ET)

Schema: Consolidated NBBO (Databento “TCBBO” family).

Fields stored (curated):

ts_event (exchange timestamp), ts_recv (receipt timestamp)

option_id (instrument_id), raw_symbol, expiration, strike, right (C/P)

bid_px, ask_px, bid_sz, ask_sz (consolidated)

underlying_mid (joined from equity feed or approximated)

derived: mid, spread, microprice, implied vol (IV), delta, gamma, vega, theta (computed)

liquidity: volume, open interest (from statistics schema; OPRA publishes start-of-day OI before RTH open).

Tier 2 — Raw archive (for forensic fills + future research)

Store compressed DBN or Parquet chunks for selected expirations / windows.

Databento Python client supports Parquet transcoder + chunked iterator for large files.

Compression: zstd recommended for historical requests.

Storage layout (local, reproducible)

/mnt/bulk/spy_verticals/ (HDD): raw archives, large Parquet partitions

/mnt/data/spy_verticals/ (NVMe): curated “hot” datasets + DuckDB files

Partitioning (curated):

dataset=opra_chain_1m/year=YYYY/month=MM/day=DD/expiration=YYYYMMDD/right=P|C/

This lets you query “all 30–45 DTE puts” efficiently.

Query engine:

DuckDB for local analytics / backtest queries (fast on Parquet).

Corporate actions and dividends

Assignment risk for short calls increases around ex-dividend dates. You need a reliable dividend calendar:

Use Databento corporate actions dataset for dividends, ex-div, record, pay dates.

Also ingest adjustment factors for consistent back-adjusted equity history.

B3) Research / backtest engine specification
Core principle: backtest the exact tradable object

A vertical spread is a 2-leg portfolio with:

Same expiration

Same right (P or C)

Different strikes

One long, one short

Spread object (canonical)
VerticalSpread(
  underlying="SPY",
  exp=YYYY-MM-DD,
  right="P"|"C",
  short_strike=K1,
  long_strike=K2,
  qty=1,
  entry_ts=...,
  entry_price_net=credit_or_debit,
  exit_rules=...
)

Fill model (NBBO-aware)

Backtest must model:

Bid-ask of each leg

Net spread price

Execution friction (you rarely get mid consistently)

Baseline fill simulation (v1, correct & conservative):

Compute each leg’s mid = (bid+ask)/2 at entry timestamp.

Compute theoretical net mid:

Credit spread (sell K1, buy K2): credit_mid = mid_short - mid_long

Debit spread (buy K1, sell K2): debit_mid = mid_long - mid_short depending on legs

Apply slippage:

filled_price = mid - α * (net_bid_ask/2) for sells

filled_price = mid + α * (net_bid_ask/2) for buys
where α starts at 0.5 and is calibrated to match IBKR live markouts.

Hard rule: if either leg has missing/locked/crossed quotes or spread too wide, no fill.

Calibration (v2):

Use historical data to measure “markout” after simulated fill:

markout(1m, 5m, 30m) vs quoted mid

Fit α as a function of liquidity/volatility state.

Fees model (IBKR-specific)

Backtest includes:

Base commission tiers + $1 min per order

Min applied per combo leg

ORF $0.02375/contract

OCC clearing $0.025/contract

Transaction fees on sell orders (SEC + FINRA TAF) and exchange fees variable.

Also:

Exercise/assignment commissions not charged for US options per IBKR page.

Assignment & exercise modeling (SPY-specific risk)

SPY options are American-style (can be exercised/assigned before expiration). For ETFs, OCC notes American-style exercise and generally $1 strike intervals.

Cboe explicitly contrasts: XSP is European-settled; SPY is American-style with early assignment risk.

Backtest policy (safe + realistic):

Strategy never holds short legs into expiration week unless explicitly allowed.

Close positions:

at profit target, or

when short strike becomes near-the-money / ITM with low extrinsic, or

before ex-dividend if short calls could be assigned (extrinsic < dividend heuristic).

Dividend early exercise intuition (industry standard): early exercise of calls is more likely when extrinsic value < dividend.

Implementation: deterministic “assignment risk” guardrail:

If short call ITM and next ex-div date ≤ 2 business days and extrinsic < dividend_estimate → force exit before close.

SPY dividend effect on underlying price

Backtest underlying series should reflect dividends and corporate actions (at minimum, adjust total return features or at least use dividend calendar for risk controls). Use Databento corporate actions/adjustment factors.

B4) Strategy logic — EXACT verticals, rules, sizing, kill-switches
Allowed positions (strict)

Only single-expiration vertical spreads:

Put credit spread (PCS): Sell put K1, Buy put K2 where K2 < K1

Call credit spread (CCS): Sell call K1, Buy call K2 where K2 > K1

Call debit spread (CDS): Buy call K1, Sell call K2 where K2 > K1

Put debit spread (PDS): Buy put K1, Sell put K2 where K2 < K1

No diagonals/calendars. No iron condors (two verticals simultaneously) in v1.

DTE ranges (default policy; configurable + optimized in research)

Credit spreads: 21–45 DTE

Debit spreads: 7–21 DTE

Rationale:

Short-vol strategies are motivated by variance risk premium evidence (options embed compensation for bearing volatility risk).

But the system will learn the best DTE bucket via walk-forward optimization rather than rely on lore.

Strike selection targets (default)

We operate on delta-based moneyness (computed from IV and underlying spot):

Credit spreads

PCS (bullish/neutral): short put Δ ≈ 0.10–0.20

CCS (bearish/neutral): short call Δ ≈ 0.10–0.20

Width: $1 or $2 for small accounts; $5 for larger

Min credit filter: credit must exceed (fees × 3) to justify the trade.

Debit spreads

CDS (bullish): long call Δ ≈ 0.55–0.65, short call Δ ≈ 0.30–0.40

PDS (bearish): long put Δ ≈ -0.55 to -0.65, short put Δ ≈ -0.30 to -0.40

Width chosen so debit ≤ risk budget.

Entry timing (hands-off friendly)

Default decision windows:

11:40 ET: primary entry window (reduces open volatility; aligns with institutional convention of using mid-day pricing windows—Cboe PUT methodology references a pricing window 11:30–12:00 ET for index putwrite index calculations).

15:30 ET: secondary window (only if risk allows and strong signal)

The engine runs continuously, but only acts at windows unless emergency exits.

Exit rules (unambiguous)

Credit spreads

Take profit: close at 50% of max profit (credit captured)

Stop loss: close if spread value reaches 2.0× entry credit (loss ~1.0× credit), OR if model risk flips sharply

Time exit: close at DTE ≤ 10 (avoid gamma + assignment)

Dividend guardrail: close call credit spreads if short call enters assignment-risk region (see below)

Debit spreads

Take profit: close at +75% return on debit OR at model-defined target probability

Stop loss: close at -50% of debit

Time exit: close at DTE ≤ 5

Assignment handling (production rulebook)

If assignment occurs anyway (because the world is messy):

Detect assignment on next account sync.

If short leg assigned and you hold stock:

If long leg exists: exercise long leg (if appropriate) or close stock immediately with marketable order.

Freeze new entries until exposure normalized.

Record incident, trigger alert.

Regime detection (used as gating, not as the “alpha”)

Regime features:

Realized vol (5d/20d), vol-of-vol

Trend (20/50/200 MA slope), drawdown

IV level and skew from option surface snapshot

Regime engine outputs:

RISK_ON, RISK_OFF, CRASH, MEAN_REVERT, VOL_EXPANSION, VOL_CRUSH

Strategy mapping:

VOL_EXPANSION / RISK_OFF: prefer debit spreads (defined-risk convexity)

VOL_HIGH but stable / MEAN_REVERT: prefer credit spreads (premium harvesting) but only if tail-risk score acceptable

Risk management (hard constraints)

Portfolio constraints (live default for $1k):

Max simultaneous positions: 1

Max defined risk (max loss) across open positions: ≤ 25% NAV

Max daily realized loss: ≤ 10% NAV → stop trading until next day

Max weekly drawdown: ≤ 25% → stop trading for 5 sessions, force model recalibration check

Position sizing

Integer contracts only.

Objective: maximize expected log growth subject to drawdown constraints.

For each candidate spread i, compute:

expected net return (after fees)

predicted 5% loss quantile (CVaR proxy)

worst-case loss (defined by width or debit)

Choose size = argmax utility among feasible sizes; with $1k, this will almost always be 1 contract.

Stress tests at decision time

Shock underlying by ±2%, ±4%, ±6% overnight gap

Shock IV by +10 vol points

Shock bid-ask widening ×2
If projected breach of max loss/day or max risk, veto trade.

Kill-switches

Data quality failure (stale quotes, missing chain) → NO TRADE

Execution anomalies (repeated rejects) → NO TRADE + alert

Model drift flags (see monitoring) → degrade to rules-only conservative mode or stop

B5) Modeling (ML) — features, labels, leakage controls, walk-forward
What the model predicts (do not overreach)

The model does not predict SPY direction directly. It predicts, for a specific candidate spread:

Expected net P&L over the planned holding period (after fees + expected slippage)

Downside tail metric (e.g., 5% quantile or expected shortfall proxy)

Probability of hitting exit conditions (profit take / stop / time exit)

This is the only honest way to do “ML for spreads” without turning it into fragile lottery-ticket behavior.

Candidate generation (the “action space”)

At each entry window:

Pull chain snapshot for DTE bucket(s).

Build candidate list:

PCS candidates: short put deltas in [0.10, 0.20], widths in {1,2,5}

CCS candidates: short call deltas in [0.10, 0.20], same widths

CDS/PDS candidates: ATM-ish long leg (|Δ|~0.6), widths chosen by debit budget

Filter for liquidity:

bid-ask spread thresholds

min open interest and volume (from OPRA statistics; OPRA publishes daily stats).

Features (high-signal, low-leakage)

Group A — Underlying state

Returns: 1d/5d/20d

Realized vol: 5d/20d (close-to-close)

Trend: MA slopes, drawdown, distance to MA

Intraday proxy (if using minute data): opening range break, midday drift

Group B — Surface state (from chain snapshot)

ATM IV, IV rank/percentile

Skew: 25Δ put IV − 25Δ call IV

Term structure: IV(30d) − IV(7d)

Local curvature / smile parameters (SVI-lite fit optional)

Group C — Spread-specific structure

Net credit/debit, width, max loss

Break-even distance

Greeks of the spread (approx delta/gamma/theta/vega)

Liquidity cost proxy: sum leg spreads, NBBO depth

Group D — Calendar / event risk

Days to expiration, day-of-week

Dividend proximity (ex-div in next N days) from corporate actions dataset

Macro event flags (optional; can be later)

Labels (what “good” means)

For each candidate spread placed historically at timestamp T:

Simulate entry fill (NBBO-aware).

Apply exit rules (profit/stop/time/dividend guardrail).

Compute realized P&L net of fees and slippage.

Labels:

y_pnl: net dollars

y_return: pnl / max_risk

y_dd: max adverse excursion

y_hit_profit, y_hit_stop

Model choice (state of the art but practical)

Primary model: Gradient boosted trees (LightGBM/CatBoost class) for tabular features

Fast training locally

Handles nonlinearities + interactions

Robust with limited history vs deep nets

Outputs:

Quantile regression for downside tail (5%/50%/95%)

Mean prediction for EV

Calibration & uncertainty:

Conformal calibration on rolling windows for quantiles

Reliability curves for probabilities (profit/stop)

Leakage controls (non-negotiable)

Time-based splits only (walk-forward)

Purge overlap: if holding period is up to 21 days, purge ±21 days around test boundaries

Use only features available at T (no future IV surface, no future OI, etc.)

Explicit “data timestamp alignment” rules:

features time = 11:40 snapshot

labels computed using subsequent data only

Walk-forward design (what you actually run)

Monthly walk-forward:

Train on last N months (e.g., 24–60 months depending on data availability)

Validate on next 1 month

Roll forward

This will also produce the DTE/width hyperparameters:

Evaluate performance across DTE buckets and widths

Select robust region, not single best

B6) Execution design (IBKR) — order construction, retries, reconciliation
Broker API interface choice (Python)

ib_insync is widely known but is archived/read-only (Mar 14, 2024).

Preferred: ib_async (modern replacement) for a cleaner async interface.

Keep a clean adapter so you can swap to IBKR’s Sync Wrapper when it matures (currently beta).

Contract identification (must be conId-based)

IBKR best practice: use only conId + exchange to define contracts, because conIds are static.

So the pipeline maintains a local mapping:

SPY_underlying_conId

option_conId per (exp, strike, right)

Combo order construction (vertical spread as BAG)

IBKR complex orders lesson describes:

secType = BAG

legs added as ComboLeg with conId, ratio, action, exchange SMART

This is the same structure used for options spreads.

Order type standardization:

Use LMT orders only in v1 for control and backtest alignment.

Consider REL/REL+MKT or Adaptive later only after extensive paper tests.

Routing and fill expectations

Use SMART routing by default.

IBKR notes that when routing to avoid exchange fees, IBKR will guarantee a fill at the NBBO at the time it routed the order (in those cases).

You still cannot assume constant NBBO fills; treat as occasional.

Idempotency + retries (SRE-grade)

Every order attempt has:

Deterministic client_order_id (hash of strategy_id + timestamp bucket + spread definition)

Local state machine:

CREATED → SUBMITTED → ACKED → PARTIAL → FILLED | CANCELLED | REJECTED

If retrying after disconnect:

reconcile open orders by matching client_order_id tags (or local mapping)

do not double-submit

Order pacing / OER

IBKR warns about Order Efficiency Ratio; automated systems can be impacted if they submit many orders without execution.

Therefore:

Maximum 1–3 order submissions per entry window

No “chasing” via frequent modifications

If not filled within X seconds/minutes → cancel and NO TRADE unless still valid

B7) Observability (logs, metrics, dashboards, alerts)
Logging (structured, immutable)

JSON logs per service:

data_ingest.log

signal_engine.log

execution.log

recon.log

Every log line includes:

timestamp, run_id, git_sha, config_hash

symbol, spread_id

decision (TRADE/NONE) + reason codes

Metrics (Prometheus-style)

Data freshness (seconds since last quote)

Candidate count per run

Model latency

Fill rate, time-to-fill

Slippage vs expected

Realized vs predicted P&L (calibration monitor)

Drawdown, exposure, margin usage

Alerts (local)

Desktop notification + email optional (your choice)

Triggers:

missed data window

repeated order rejects

unexpected positions (assignment)

daily loss limit hit

model drift

Post-trade analytics

Trade ledger with:

entry/exit NBBO snapshot

predicted vs realized EV, tail quantile

attribution: delta P&L vs IV P&L proxy

B8) Security & reliability (local, production-safe)
Secrets management

No API keys in code.

Use:

.env for dev

systemd environment file for prod service

Databento recommends API key via DATABENTO_API_KEY environment variable.

Reproducibility

Lock dependencies with uv or pip-tools

Pin model artifacts with versioning:

model_id = hash(training_data_range + params + code_sha)

Store:

training manifest

feature definitions

backtest config

Docker hardening (optional)

Run services in Docker with:

read-only filesystem where possible

drop capabilities

bind-mount only needed directories

Network egress allow-list (Databento + IBKR only)

C) Implementation Blueprint (Actionable)
C1) Repo layout (SOT)
spy-verticals/
  README.md
  pyproject.toml
  uv.lock (or requirements.lock)
  .env.example
  configs/
    base.yaml
    paper.yaml
    live_1k.yaml
    data.yaml
    risk_profiles.yaml
    ibkr.yaml
  data/
    duckdb/
    curated/
    raw/
  src/
    common/
      time.py
      calendars.py
      logging.py
      types.py
      config.py
    databento/
      client.py
      opra_ingest.py
      reference.py
      symbology.py
      corporate_actions.py
      definitions.py
    marketdata/
      chain_snapshot.py
      greeks.py
      iv_solver.py
      surface.py
      liquidity.py
    backtest/
      engine.py
      fills.py
      fees_ibkr.py
      assignment.py
      metrics.py
      reports.py
    strategy/
      candidates.py
      rules.py
      regimes.py
      policy.py
      sizing.py
      exits.py
      decision.py   # outputs TRADE NOW / NO TRADE + parameters
    models/
      dataset.py
      features.py
      train.py
      validate.py
      calibrate.py
      infer.py
      registry.py
    execution/
      ibkr_adapter.py
      orders.py
      combo.py
      reconcile.py
      safety.py
    observability/
      metrics.py
      dashboard.py
      alerts.py
    cli/
      main.py
  tests/
    unit/
    integration/
  notebooks/
  scripts/
    bootstrap_db.sh
    run_backtest.sh
    run_paper.sh
    run_live.sh
  docs/
    SOT.md   # this document
    runbooks/
      incident_assignment.md
      incident_data_outage.md
      incident_ibkr_disconnect.md

C2) Golden path commands (Ubuntu 24.04)
Local venv (recommended for speed)
git clone <repo>
cd spy-verticals

python -m venv .venv
source .venv/bin/activate
pip install -U pip wheel

pip install -e ".[dev]"
cp .env.example .env
# set DATABENTO_API_KEY and IBKR creds in .env (never commit)

# 1) Build curated datasets
python -m spy_verticals.cli.main ingest-opra --start 2023-03-28 --end 2026-02-02

# 2) Run backtest
python -m spy_verticals.cli.main backtest --config configs/paper.yaml

# 3) Train model
python -m spy_verticals.cli.main train --config configs/paper.yaml

# 4) Paper trade loop
python -m spy_verticals.cli.main trade --mode paper --config configs/paper.yaml

# 5) Live signals (no auto)
python -m spy_verticals.cli.main trade --mode live --confirm --config configs/live_1k.yaml

Docker (optional)

Useful if you want identical environments and systemd services.

C3) Staged rollout plan with go/no-go gates
Stage 0 — Backtest correctness (Gate: “Simulator is honest”)

Exit criteria:

Fee model matches IBKR schedule including combo-leg minimums

Spread P&L matches hand calculations on sampled trades

Assignment guardrails tested against dividend calendar scenarios

Stage 1 — Paper trading stability (Gate: “Operationally stable”)

Run 4–8 weeks in IBKR paper with the same cadence and code path.
Exit criteria:

No orphan orders

Reconciliation matches positions daily

Live slippage distribution is within backtest tolerance bands

Decision engine produces deterministic signals per window

Stage 2 — Live “signals only” with $1k (Gate: “Can survive”)

Policy: 1 trade max, max risk ≤ 25% NAV, hard daily stop ≤ 10% NAV.
Exit criteria (4–12 weeks):

No loss-limit violations

Realized slippage consistent

Model calibration stable (no massive drift)

Drawdown within acceptable threshold

Stage 3 — Live auto-execution (optional)

Only after Stage 2 passes.

C4) Minimal v1 (still correct & safe)

v1 includes:

Databento OPRA ingestion (minute consolidated NBBO snapshots)

IBKR combo order placement (BAG + ComboLeg)

Realistic fees including per-leg minimum

Rules-only baseline + regime gating

Signal output: TRADE NOW with full order parameters

v1 explicitly excludes:

Tick-level queue simulation

Deep learning / RL

Complex multi-position portfolios

C5) Roadmap (prioritized improvements)

Fill calibration via markouts (reduce backtest optimism)

Model upgrade to quantile + conformal (better tail control)

Dynamic width selection (1 vs 2 vs 5 based on expected utility)

Add dividend/assignment risk scoring to spread selection

Databento live OPRA integration for cleaner quotes (optional; costs)

Execution enhancements (Adaptive/REL orders after proving benefit)

Drift detection + automatic de-risk mode

Local dashboard (FastAPI) for monitoring

D) Evaluation + Red Team
D1) Success metrics (beyond PnL)

Return & risk

CAGR, volatility, Sharpe, Sortino

Max drawdown, time-to-recover

Tail: 1% and 5% worst weeks, CVaR proxy

Trade quality

Net expectancy per trade (after fees)

Profit factor, hit rate, avg win/loss

Slippage distribution vs predicted

Fill rate and time-to-fill

Regime performance

Metrics broken down by regime label (RISK_ON/OFF, vol expansion/crush)

Operational

Uptime, data freshness

Order rejects, disconnect rate

Reconciliation mismatches

D2) Top 20 failure modes + mitigations

Fee under-modeling (combo min per leg ignored) → bake into simulator + unit tests

Spread fill optimism → conservative α, calibrate with markouts

Bid-ask widening during stress → stress tests widen ×2 and veto

Dividend assignment on short calls → dividend calendar + extrinsic check

Holding too close to expiry (pin/contra) → mandatory time exit

Bad chain snapshot alignment → strict timestamp policy and audits

Model leakage → purged walk-forward, audit feature timestamps

Regime shift → drift detection, degrade to conservative rules

Overfitting DTE/width → select robust region, not single best

Data vendor schema change → versioned parsers; Databento schema migration awareness

IBKR disconnects → reconnect logic + reconciliation

Duplicate orders → idempotent client_order_id + reconcile

Order pacing / OER violation → cap order churn

Stale quotes → freshness checks, NO TRADE

Illiquid strikes → OI/volume filters + spread width caps

Model outputs miscalibrated → rolling calibration + alarms

Local disk fills → storage quotas + rotation; archive to HDD

Clock/timezone bugs → exchange calendar library + UTC normalization

Corporate actions mismatch → rely on reference data and adjustment factors

Final note (non-negotiable truth)

A $1,000 options-spread account paying per-leg minimum commissions will not support “high frequency premium harvesting” profitably. The only viable route is selective trading with large enough expected edge per trade, strict risk controls, and a system that can survive long enough for the edge (if any) to compound.
