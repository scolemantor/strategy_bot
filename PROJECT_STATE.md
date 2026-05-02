# strategy_bot — Project State

> Living document. Update at the end of every working session.

**Last updated:** 2026-05-02 (post-Phase-3-merge)
**Repo:** github.com/scolemantor/strategy_bot
**Account:** Alpaca paper, $200k notional, seeded 2026-05-01 11:57am ET

---

## TL;DR

- **Phase 1, 2, 3:** DONE. Phase 3 merged to main 2026-05-02.
- **Phase 4:** IN PROGRESS. Scanner #1 (insider_buying) currently validating against live SEC data.
- **Phases 5-12:** not started, fully detailed below.
- **181 tests passing.**
- **V3 portfolio:** 7.4% CAGR, -17.2% max DD, Sharpe 0.68 over 5y. Beats SPY's risk-adjusted return for the period.

---

## Current state of the bot

**Portfolio V3 (locked in YAML):**
- Trunk 70%: VTI 80% / BIL 10% / GLD 10%
- Branches 20%: SMH, XLU, ITA, IHI, PAVE, INDA, COPX (inverse-vol weighted)
- Acorns 10%: cash, manually managed

**Live behavior:**
- Vol weighting on branches with iterative water-fill clipping (5%/40% bounds, 90-day window)
- Regime detection on SPY 200dma with 2% buffer + 3 consecutive days of confirmation
- Defensive tagging: BIL and GLD held flat during regime-off (only equity scales)
- Auto-liquidation of held positions outside YAML allocation
- Tax-aware lot ledger (HIFO + long-term preference + losses-first) live and seeded against paper account

**Paper account positions (as of 2026-05-01):** Still holds VXUS and BND from V1 seeding. Auto-liquidation will sell them at next executed rebalance window. Risk manager will reject single-shot liquidation in real money since each is ~5% of portfolio — wind-down logic needed before live (see Phase 7).

---

## Cross-machine workflow

- **Code:** GitHub. Pull at session start, commit + push at session end.
- **Cache:** `data_cache/` symlinked to `G:\My Drive\strategy_bot_cache\data_cache\` on both machines.
- **Scan output:** `scan_output/` symlinked to `G:\My Drive\strategy_bot_cache\scan_output\` on both machines (added 2026-05-02).
- **`.env`:** machine-local, never synced. Paper keys regenerated 2026-05-01 after accidental exposure.

---

# PHASE PLAN — full detail (12 phases)

## ✅ Phase 1: Live Skeleton — DONE

CLI rebalancer with Alpaca integration.
- `[x]` Project structure (src/, config/, tests/)
- `[x]` Pydantic config validation
- `[x]` Alpaca broker wrapper
- `[x]` Oak rebalancer (target weights, drift, order generation)
- `[x]` Risk manager (position size cap, market-hours check, drawdown kill switch)
- `[x]` Order executor with dry-run default
- `[x]` CLI: `python main.py status`, `python main.py rebalance [--execute] [--seeding]`
- `[x]` Live-trading confirmation prompts
- `[x]` Validated: paper account seeded 2026-05-01, 11 positions filled at 0% drift

## ✅ Phase 2: Backtest Harness — DONE

- `[x]` Historical bar fetcher with parquet caching
- `[x]` Backtest engine with slippage simulation (5bps)
- `[x]` Performance stats (CAGR, Sharpe, max drawdown, vol)
- `[x]` Buy-and-hold benchmark comparison
- `[x]` CSV output (equity_curve.csv, trades.csv)
- `[x]` CLI: `python backtest.py [--start] [--end] [--capital] [--frequency] [--benchmark]`

## ✅ Phase 3: Strategy Enhancements — DONE (15 commits, merged 2026-05-02)

- `[x]` Volatility-weighted position sizing with iterative water-fill clipping
- `[x]` Regime detection: SPY 200dma overlay with 2% buffer + 3-day confirmation
- `[x]` Defensive tagging: per-holding `risk_class: equity | defensive`
- `[x]` Backtest engine wired to pass historical_prices
- `[x]` Live rebalancer wired to fetch historical_prices (was previously running with regime always-ON, equal-weight branches)
- `[x]` V3 portfolio retune: 70/20/10 sleeves, drop VXUS, BND→BIL
- `[x]` Tax-aware lot ledger (SQLite, immutable lots + append-only consumptions)
- `[x]` Tax-aware lot selection (ST loss → LT loss → LT HIFO → ST HIFO)
- `[x]` Migration + reconciliation against broker positions
- `[x]` Wire ledger into executor and main loop
- `[x]` Auto-liquidate held positions outside YAML allocation
- `[x]` Phase 3 deferred: drawdown circuit breaker DROPPED (regime detection covers same need)

## 🔄 Phase 4: Acorns Idea-Generation Scanner — IN PROGRESS

Architecture is set. Output is read-only CSVs. No scanner places orders, ever.

### 4a — Individual scanners (1 of 13 buildable in progress)

- `[~]` #1 insider_buying — running 2026-05-02, validating output
- `[ ]` #2 breakout_52w — has universe-truncation bug (alphabetical cap at A-HYR cuts MSFT/NVDA/META). Fix: sort universe by market cap or dollar volume before truncating, or raise cap to full ~11,694
- `[ ]` #3 earnings_drift — needs `pip install yfinance`. ~7-8 min runtime
- `[ ]` #4 spinoff_tracker — reuses SEC EDGAR pipeline
- `[ ]` #5 fda_calendar
- `[ ]` #6 thirteen_f_changes
- `[ ]` #7 short_squeeze
- `[ ]` #8 small_cap_value
- `[ ]` #9 sector_rotation
- `[ ]` #10 earnings_calendar
- `[ ]` #11 macro_calendar
- `[ ]` #12 ipo_lockup
- `[ ]` #13 insider_selling_clusters (negative signal, exclusion list, reuses #1's cache)
- `[B]` #14 options_unusual — paid data
- `[B]` #15 crypto_onchain — paid data
- `[B]` #16 sentiment — fragmented sources
- `[B]` #17 ma_rumors — paid news API

### 4b — Investability filter

Universal quality gate every scanner result passes through.
- `[ ]` Market cap floor (configurable: $300M / $50M / $10M tiers)
- `[ ]` Average daily dollar volume minimum
- `[ ]` Recent dilution detector (>10% new shares in 90 days)
- `[ ]` Going-concern flag from latest 10-K
- `[ ]` Listing exchange filter (no OTC unless opted in)
- `[ ]` Hard exclusions list (manually maintained)
- `[ ]` Filtered-out audit trail (separate `rejected.csv`)

### 4c — Cross-scanner meta-ranker

- `[ ]` Aggregator (load all scanner CSVs, build master DataFrame keyed on ticker)
- `[ ]` Signal vector per ticker (boolean flags per scanner + per-scanner score)
- `[ ]` Composite scoring (weighted sum + multi-signal bonus + category diversity)
- `[ ]` `scan_output/<date>/master_ranked.csv` output
- `[ ]` Configurable signal weights (`config/scanner_weights.yaml`)

### 4d — Watchlist tracker

- `[ ]` Watchlist storage (`config/watchlist.yaml`)
- `[ ]` CLI: `python scan.py watch add/remove/list TICKER`
- `[ ]` Daily watchlist run with delta detection
- `[ ]` Watchlist-only digest separate from main scan results
- `[ ]` Auto-removal rules (filter failures, N days of silence)

### 4e — Backtested signal weighting (REQUIRED for Phase 11 autonomy)

- `[ ]` Per-scanner historical edge measurement framework
- `[ ]` Backtest sweep tooling (replay scanner over historical data, measure forward returns at 1d/1w/1mo/3mo/6mo)
- `[ ]` Bayesian weighting of signals based on measured edge
- `[ ]` Drift detection (signals that decay over time)
- `[ ]` Output: per-scanner edge report with confidence intervals

### 4f — Phase 3 retroactive items (added to Phase 4)

These got identified during Phase 3 work and need to land before Phase 5 or they'll keep biting.
- `[ ]` Phase 3 regime params backtest sweep: try (buffer, days) combinations of (0.01, 2), (0.02, 3), (0.03, 5), (0.05, 5). Pick combination that maximizes Sharpe net of tax drag.
- `[ ]` Trunk allocation re-evaluation after Phase 4 scanners are producing alpha. If scanner-driven branches don't add edge, revisit whether 70/20/10 is still right.

## ⬜ Phase 5: Persistent Logging

Bot currently logs to terminal only. Won't work on a server.
- `[ ]` Structured JSON Lines logging to disk (one file per day: `logs/2026-05-01.jsonl`)
- `[ ]` Every event: timestamp, severity, component, action, before/after state, request IDs
- `[ ]` Order audit log: dedicated `logs/orders.jsonl`, never auto-rotated
- `[ ]` Scanner output history: every scan archived to `scan_output/archive/`
- `[ ]` Rebalance history: each run gets a JSON record with proposed orders, executed orders, drift state, regime at time of run
- `[ ]` Log rotation: daily files, gzipped after 7 days, deleted after 90 days (configurable)
- `[ ]` Log query CLI: `python logs.py query --action order --since 2026-04-01 --ticker VTI`
- `[ ]` Critical events stay forever (orders, kill-switch trips, errors)
- `[ ]` Sensitive fields redacted (no API keys, no full account numbers)

## ⬜ Phase 6: Alerting & Notifications

Need to know when things happen without watching a terminal. Required for Phase 7 deployment.
- `[ ]` Notification channel config: email, Slack, both, or other (Discord, Telegram, ntfy.sh)
- `[ ]` Critical alerts (always on):
  - Kill switch tripped
  - Drawdown threshold breached
  - Order placement failed
  - Authentication failure
  - Unhandled exception in any phase
- `[ ]` Operational alerts (configurable):
  - Daily summary (portfolio value, top movers, drift, regime, scanners run)
  - Rebalance occurred (orders, fills, slippage)
  - Scanner finished with N candidates above threshold
  - Watchlist saw new signal
- `[ ]` Daily digest format (human-readable email)
- `[ ]` Slack integration via webhook
- `[ ]` Email integration via SMTP (Gmail app password)
- `[ ]` Quiet hours config (user is night owl, may invert default)
- `[ ]` Rate limiting (no more than N alerts/hour)
- `[ ]` Alert deduplication (same error within 1hr bundled)
- `[ ]` Test mode: `python alerts.py test`

## ⬜ Phase 7: Production Deployment

- `[ ]` Persistent state store (drawdown high-water mark, last-run timestamps, request IDs)
- `[ ]` Dockerfile (one-command deploy)
- `[ ]` Cloud VM provisioning (Hetzner or DigitalOcean ~$5-10/mo)
- `[ ]` Cron schedule (status daily, rebalance weekly, scanners daily)
- `[ ]` Health check endpoint / dead-man switch

### 7a — Phase 3 retroactive items (added to Phase 7, required pre-live)

These showed up during Phase 3 work but only matter when going to real money.
- `[ ]` Fill-status polling: replace quote-based ledger writes with actual fill confirmations. Currently the ledger records orders at the latest quote price when the broker accepts the order, not the actual fill. Paper-grade — real money needs accurate cost basis.
- `[ ]` VXUS/BND wind-down logic: risk manager will reject single-shot liquidation in real money (each is ~5% of portfolio). Need a feature that recognizes "this position is being exited entirely" and paces sells across multiple rebalance windows. Required before any rebalance that would exit a position.

### 7b — Pre-live gates

- `[ ]` 30-day paper observation period (no crashes, no missed rebalances, no surprise behavior)
- `[ ]` Real-money go-live with $5-10k slice first
- `[ ]` 30+ clean days at small size
- `[ ]` Scale to full $200k

## ⬜ Phase 8: Operational Hardening

- `[ ]` Test failure modes deliberately:
  - Alpaca down
  - Bad quote
  - Broker reject
  - VM reboot mid-rebalance
  - Network partition
  - Disk full
- `[ ]` Manual override interface:
  - Pause bot
  - Force-rebalance
  - Veto specific orders
  - Read-only mode
- `[ ]` Multi-account support (taxable + IRA + Roth)
- `[ ]` Tax-loss harvesting:
  - Lot-level cost basis tracking (DONE in Phase 3)
  - Wash-sale tracking (30-day window across accounts)
  - Substitute-asset swap pairs (VTI ↔ ITOT, etc.)
  - Auto-harvest opportunistic losses (currently only does opportunistic during normal rebalance)

## ⬜ Phase 9: Branches Signal Overlay

Momentum filter on conviction holdings.
- `[ ]` Signal definitions (50/200dma cross, 12-1 momentum rank, RSI<30 cooldown)
- `[ ]` Per-branch overlay logic (each of 7 branches has own threshold)
- `[ ]` Three response modes: skip-buys, reduce-target, full-exit
- `[ ]` Backtest across regimes (2018 vol, 2020 crash, 2022 bear, 2023 rally)
- `[ ]` Configuration: `config/branch_signals.yaml`
- `[ ]` Logging when overlay triggers non-rebalance
- `[ ]` Override flag for buy-the-dip moments

**Note:** Backtests will look great in-sample, may not help live. Treat with skepticism.

## ⬜ Phase 10: Portfolio-Level Vol Targeting

Continuous (vs regime's binary) scaling of total equity exposure.
- `[ ]` Vol measure choice (30d/60d realized, EWMA, VIX-implied)
- `[ ]` Target vol setting (12% moderate / 8% conservative)
- `[ ]` Scaling logic (`equity = target / current`, capped 0.3x-1.5x)
- `[ ]` Update cadence (daily/weekly/monthly)
- `[ ]` Implementation: scale all sleeve weights proportionally, remainder to cash
- `[ ]` Backtest comparison vs static V3 across regimes
- `[ ]` Interaction rules with regime detector (don't compound badly)
- `[ ]` Tax friction analysis

**Note:** Probably IRA-only due to short-term capital gains in taxable accounts.

## ⬜ Phase 11: Acorns Sleeve Automation — High Research Bar

Currently fully manual. This phase explores semi-automating.
- `[ ]` Define rules engine (when does bot auto-allocate acorns capital?)
- `[ ]` Position sizing per acorn (equal weight? Score-weighted? Bound max 1% of total)
- `[ ]` Max acorn count cap (10-20 simultaneous)
- `[ ]` Exit rules (stop loss, time-based, score decay, thesis violation)
- `[ ]` Cooldown between acorns (1-2/week max)
- `[ ]` Manual veto window (bot proposes EOD, executes at open unless vetoed)
- `[ ]` Backtest the rules (multi-month research effort)
- `[ ]` Paper-trade only for 90+ days
- `[ ]` Hard kill switch (acorns sleeve drawdown >30% freezes new buys)

**Note:** Highest-risk feature. Recommend: build Phase 4 manual workflow, run for 6+ months, get feel for which signals predict winners in your hands, THEN encode judgment into rules.

## ⬜ Phase 12: Custom Benchmarks

- `[ ]` Benchmark presets (60/40, AOR, VTI/BND mixes)
- `[ ]` Risk parity benchmark
- `[ ]` Custom benchmark composer (`--benchmark "VTI:0.5,BND:0.3,GLD:0.1,VXUS:0.1"`)
- `[ ]` Multi-benchmark output
- `[ ]` Risk-adjusted comparisons (Sortino, info ratio, max-DD-adjusted return)
- `[ ]` Tracking error and information ratio
- `[ ]` Rolling-window comparisons

---

# Two-Month Sequencing (May 2 → July 2)

Realistic, not aspirational.

### Weeks 1-3 (May 2 → May 23) — Phase 4a sprint
- Validate insider_buying output (in progress 2026-05-02)
- Fix breakout_52w universe truncation
- Build scanners #3-13 at ~1-2 per session
- Realistic: 8-10 scanners shipped, 3-5 deferred to later

### Week 4 (May 23 → May 30) — Phase 4b/c
Investability filter and cross-scanner meta-ranker. Without these, scanner CSVs are noise lists.

### Week 5 (May 30 → June 6) — Phase 4e + 4f
Backtested signal weighting framework (4e). Phase 3 retroactive items (4f): regime params sweep, trunk re-eval if scanners are producing edge.

### Weeks 6-7 (June 6 → June 20) — Phases 5 & 6
Persistent logging, then alerting. Both required before Phase 7.

### Weeks 8-9 (June 20 → July 4) — Phase 7 prep + start observation
Dockerfile, VM, cron, health checks. Fill-status polling (7a). VXUS/BND wind-down logic (7a). Start 30-day paper observation period when 7 ships.

### What this two-month plan does NOT include
- Phase 4d (watchlist) — defer
- Phase 8 (hardening) — runs in parallel with paper observation
- Phases 9-12 — post-go-live

### Real-money go-live earliest plausible date
- Phase 7 ships ~July 1
- 30-day paper observation: July 1 → Aug 1
- $5-10k slice: Aug 1
- 30 days at small size: Aug 1 → Sept 1
- Full $200k: ~Sept 1

**Aggressive but plausible: late August. Realistic: September.**

---

# How to resume next session

1. `cd "C:\Users\Sean Coleman\strategy_bot"`, `git pull origin main`
2. Read this file to get caught up
3. Check `scan_output/` for any new scanner CSVs (Drive-synced from either machine)
4. Run `python -m pytest tests/ -q` to confirm 181 tests still passing
5. Pick from the current phase's checklist
6. End of session: update this file's "Last updated" timestamp + checkboxes, then commit

---

# Open risks / known issues

- **Phase 4 ROI question.** 13 scanners feeding a 10% acorns sleeve = $20k of capital. That's a lot of pipe for a small slice. Justification depends on either (a) educational/transferable infrastructure, or (b) eventually flipping Phase 11 autonomy. If (b), 4e is hard-required.
- **V3 underperforms SPY by 4 CAGR.** By design (defensive portfolio). But if scanner-driven branches don't add edge, the trunk allocation should be revisited (4f).
- **Single-period backtest.** 7.4% / -17.2% / 0.68 are 2021-2026 numbers. Different regimes will produce different numbers. Don't anchor.
- **No tax drag in backtests.** 277 trades over 5y is real tax events. After-tax CAGR is lower than 7.4%.
- **Quote-based ledger writes.** Paper-grade. Real money needs fill-status polling (7a).
- **VXUS/BND still in paper account.** Auto-liquidation will sell them at next executed rebalance. Risk manager rejects single-shot in real money — need wind-down feature first (7a).
- **breakout_52w universe truncation.** Alphabetical cap at A-HYR misses MSFT/NVDA/META. Fix early in Phase 4 sprint.
- **Phase 11 fundamentally hard.** Most retail systematic strategies fail at the rules-encoding step. Treat with extreme skepticism.
