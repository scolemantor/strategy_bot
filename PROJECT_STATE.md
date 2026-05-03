# strategy_bot — Project State

> Living document. Update at the end of every working session.

**Last updated:** 2026-05-03 (morning, post-Phase-4a-night-1)
**Repo:** github.com/scolemantor/strategy_bot
**Account:** Alpaca paper, $200k notional, seeded 2026-05-01 11:57am ET

---

## TL;DR

- **Phases 1, 2, 3:** DONE. Phase 3 merged 2026-05-02.
- **Phase 4a:** 4 of 17 scanners shipped. Scanners 1-4 working with real candidates. 13 scanners remaining (9 free-data, 4 paid-data).
- **Phases 5-12:** not started.
- **181 tests passing.**
- **V3 portfolio:** 7.4% CAGR, -17.2% max DD, Sharpe 0.68 over 5y. Beats SPY's risk-adjusted return.

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

**Paper account positions (as of 2026-05-01):** Still holds VXUS and BND from V1 seeding. Auto-liquidation will sell them at next executed rebalance window. Risk manager will reject single-shot liquidation in real money since each is ~5% of portfolio — wind-down logic needed before live (see Phase 7a).

---

## Cross-machine workflow

- **Code:** GitHub. Pull at session start, commit + push at session end.
- **Cache:** `data_cache/` symlinked to `G:\My Drive\strategy_bot_cache\data_cache\` on both machines.
- **Scan output:** `scan_output/` symlinked to `G:\My Drive\strategy_bot_cache\scan_output\` on both machines.
- **`.env`:** machine-local, never synced. Paper keys regenerated 2026-05-01 after accidental exposure.

---

# PHASE PLAN — full detail (12 phases)

## ✅ Phase 1: Live Skeleton — DONE

CLI rebalancer with Alpaca integration. 11 positions seeded paper account 2026-05-01 at 0% drift.

## ✅ Phase 2: Backtest Harness — DONE

Historical bar fetcher with parquet caching, backtest engine with 5bps slippage, CAGR/Sharpe/DD stats, benchmark comparison.

## ✅ Phase 3: Strategy Enhancements — DONE (15 commits, merged 2026-05-02)

- Vol-weighted branches with iterative water-fill clipping
- Regime detection (SPY 200dma + 2% buffer + 3-day confirmation)
- Defensive tagging (BIL/GLD held flat during regime-off)
- V3 portfolio retune to 70/20/10 (drop VXUS, BND→BIL)
- Tax-aware lot ledger (SQLite, immutable lots, append-only consumptions, ST loss → LT loss → LT HIFO → ST HIFO selection)
- Migration + reconciliation against broker positions
- Auto-liquidation of held positions outside YAML
- Drawdown circuit breaker DROPPED (regime detection covers it)

## 🔄 Phase 4: Acorns Idea-Generation Scanner — IN PROGRESS

Architecture is set. Output is read-only CSVs. No scanner places orders, ever.

**Full 17-scanner build is locked. No scanner is being skipped, ever.** Free-data scanners ship first (4a), paid-data scanners ship after subscription decisions (4g).

### 4a — Free-data scanners (4 of 13 shipped)

- `[x]` **#1 insider_buying** — SEC Form 4 cluster buys. 19 candidates this run (CHTR, ABT, GEHC, WASH, AVLN). XSL-stripping fix applied 2026-05-02.
- `[x]` **#2 breakout_52w** — Alpaca bars, new 52w highs. 155 candidates (TWLO, NVT, MXL, BAND). Universe truncation, batching, lookback gates, sanity caps all fixed 2026-05-02.
- `[x]` **#3 earnings_drift** — yfinance, post-earnings drift. 20+ candidates including textbook PEAD: CIEN +82% in 57d on 16% beat, MU +22% in 44d on 33% beat, HPE +36% in 53d on 11% beat. S&P 500 universe + parquet cache + sanity caps applied 2026-05-02.
- `[x]` **#4 spinoff_tracker** — SEC Form 10/10-12B/10-12G. 14 candidates after 3 filters (name patterns, CIK age, parent 8-K cross-reference). Real wins: FDXF (parent FDX), HONA (parent HON), Enviri II (parent NVRI), VSNT (parent CCZ), USDW (parent IPW), Augusta SpinCo (parent WAT). Built 2026-05-03.
- `[ ]` **#5 fda_calendar** — small/mid biotech with PDUFA decisions in 30-90 day window
- `[ ]` **#6 thirteen_f_changes** — institutional holdings (curated smart-money fund list, reuses SEC pipeline)
- `[ ]` **#7 short_squeeze** — high SI + days-to-cover + positive momentum
- `[ ]` **#8 small_cap_value** — Greenblatt-style fundamentals screen
- `[ ]` **#9 sector_rotation** — sector ETF relative strength vs SPY
- `[ ]` **#10 earnings_calendar** — companies reporting next 5 trading days
- `[ ]` **#11 macro_calendar** — FOMC/CPI/NFP/Treasury auctions
- `[ ]` **#12 ipo_lockup** — IPO lockup expirations and post-expiry bounce setups
- `[ ]` **#13 insider_selling_clusters** — negative signal, exclusion list, reuses #1's cache

### 4b — Investability filter (NOT STARTED)

Universal quality gate every scanner result passes through.
- `[ ]` Market cap floor (configurable: $300M / $50M / $10M tiers)
- `[ ]` Average daily dollar volume minimum
- `[ ]` Recent dilution detector (>10% new shares in 90 days)
- `[ ]` Going-concern flag from latest 10-K
- `[ ]` Listing exchange filter (no OTC unless opted in)
- `[ ]` Hard exclusions list (manually maintained)
- `[ ]` Filtered-out audit trail (separate `rejected.csv`)

### 4c — Cross-scanner meta-ranker (NOT STARTED)

- `[ ]` Aggregator (load all scanner CSVs, build master DataFrame keyed on ticker)
- `[ ]` Signal vector per ticker (boolean flags + per-scanner score)
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

### 4f — Phase 3 retroactive items

Identified during Phase 3 work, need to land before Phase 5.
- `[ ]` Phase 3 regime params backtest sweep: try (buffer, days) combinations of (0.01, 2), (0.02, 3), (0.03, 5), (0.05, 5). Pick combination that maximizes Sharpe net of tax drag.
- `[ ]` Trunk allocation re-evaluation after scanners are producing alpha. If scanner-driven branches don't add edge, revisit whether 70/20/10 is still right.

### 4g — Paid-data scanners (NOT STARTED — subscriptions pending)

These four scanners require paid data feeds. **All four ship.** The build sequence: subscribe to feed → write client wrapper → write scanner → validate → register. Same pattern as #1-13.

Subscription decisions to be made when 4a is complete and we know which signals matter most.

- `[ ]` **#14 options_unusual** — Unusual Whales / FlowAlgo / OptionStrat
  - Signal: calls bought above ask >5x average daily volume
  - Cost: $50-200/mo
  - Build: API client → scanner → output CSV with ticker, contract, premium, OI change
- `[ ]` **#15 crypto_onchain** — Glassnode / CoinMetrics paid tier
  - Signal: on-chain accumulation patterns for major crypto + crypto-adjacent equities (COIN, MSTR, MARA, RIOT)
  - Cost: $30-100/mo
  - Build: API client → metric tracker → equity proxy mapper → output CSV
- `[ ]` **#16 sentiment** — Reddit (PRAW) + Google Trends + StockTwits unified pipeline
  - Signal: spike in retail sentiment / mention volume per ticker
  - Cost: API access mostly free, but multi-source pipeline is the engineering cost (multi-week build)
  - Build: per-source fetchers → unified ticker extraction → mention-volume baseline → spike detection
- `[ ]` **#17 ma_rumors** — Bloomberg / Reuters / Benzinga news API
  - Signal: M&A rumor mentions for buyout candidates trading below potential takeout price
  - Cost: $100-500/mo (Benzinga is cheapest, Bloomberg most expensive)
  - Build: news API client → NER for ticker + acquirer extraction → rumor-vs-confirmed classification → output CSV

## ⬜ Phase 5: Persistent Logging

Bot currently logs to terminal only. Won't work on a server.
- `[ ]` Structured JSON Lines logging to disk (`logs/2026-05-01.jsonl`)
- `[ ]` Every event: timestamp, severity, component, action, before/after state, request IDs
- `[ ]` Order audit log: dedicated `logs/orders.jsonl`, never auto-rotated
- `[ ]` Scanner output history: every scan archived to `scan_output/archive/`
- `[ ]` Rebalance history with proposed orders, executed orders, drift state, regime
- `[ ]` Log rotation (daily files, gzipped after 7d, deleted after 90d)
- `[ ]` Log query CLI: `python logs.py query --action order --since 2026-04-01 --ticker VTI`
- `[ ]` Critical events stay forever
- `[ ]` Sensitive fields redacted

## ⬜ Phase 6: Alerting & Notifications

Required for Phase 7 deployment.
- `[ ]` Notification channel config (email, Slack, both, or other)
- `[ ]` Critical alerts: kill switch, drawdown breach, order failure, auth failure, unhandled exception
- `[ ]` Operational alerts: daily summary, rebalance occurred, scanner finished, watchlist signal
- `[ ]` Daily digest email format
- `[ ]` Slack integration via webhook
- `[ ]` Email integration via SMTP
- `[ ]` Quiet hours config
- `[ ]` Rate limiting (no more than N alerts/hour)
- `[ ]` Alert deduplication
- `[ ]` Test mode

## ⬜ Phase 7: Production Deployment

- `[ ]` Persistent state store (drawdown HWM, last-run timestamps, request IDs)
- `[ ]` Dockerfile (one-command deploy)
- `[ ]` Cloud VM provisioning (Hetzner or DigitalOcean ~$5-10/mo)
- `[ ]` Cron schedule (status daily, rebalance weekly, scanners daily)
- `[ ]` Health check endpoint / dead-man switch

### 7a — Pre-live retroactive items

Showed up during Phase 3 work, only matter at real money.
- `[ ]` Fill-status polling: replace quote-based ledger writes with actual fill confirmations
- `[ ]` VXUS/BND wind-down logic: pace single-shot exits across multiple rebalance windows

### 7b — Pre-live gates

- `[ ]` 30-day paper observation period (no crashes, no missed rebalances)
- `[ ]` Real-money go-live with $5-10k slice first
- `[ ]` 30+ clean days at small size
- `[ ]` Scale to full $200k

## ⬜ Phase 8: Operational Hardening

- `[ ]` Test failure modes deliberately (Alpaca down, bad quote, broker reject, VM reboot, network partition, disk full)
- `[ ]` Manual override interface (pause, force-rebalance, veto orders, read-only mode)
- `[ ]` Multi-account support (taxable + IRA + Roth)
- `[ ]` Tax-loss harvesting: wash-sale tracking, substitute-asset swap pairs, opportunistic harvest

## ⬜ Phase 9: Branches Signal Overlay

Momentum filter on conviction holdings. Treat with skepticism.
- `[ ]` Signal definitions, per-branch overlay logic, three response modes
- `[ ]` Backtest across regimes
- `[ ]` Configuration: `config/branch_signals.yaml`
- `[ ]` Override flag for buy-the-dip moments

## ⬜ Phase 10: Portfolio-Level Vol Targeting

Probably IRA-only due to tax friction in taxable accounts.
- `[ ]` Vol measure choice, target vol setting, scaling logic, update cadence
- `[ ]` Backtest comparison vs static V3
- `[ ]` Interaction rules with regime detector

## ⬜ Phase 11: Acorns Sleeve Automation — High Research Bar

Highest-risk feature. Build Phase 4 manual workflow first, run for 6+ months, encode judgment into rules only after seeing what works.
- `[ ]` Rules engine, position sizing, max acorn count, exit rules
- `[ ]` Cooldown between acorns, manual veto window
- `[ ]` Backtest the rules (multi-month research)
- `[ ]` Paper-trade only for 90+ days
- `[ ]` Hard kill switch (drawdown >30% freezes new buys)

## ⬜ Phase 12: Custom Benchmarks

- `[ ]` Benchmark presets, risk parity, custom composer
- `[ ]` Multi-benchmark output, risk-adjusted comparisons
- `[ ]` Tracking error and information ratio
- `[ ]` Rolling-window comparisons

---

# Sequencing

### Phase 4a — free-data scanners (in progress)
4 shipped night 1. 9 left. ~1-2 per session = 5-9 more sessions.

### Phase 4b/c/d/e/f — supporting infrastructure
After 4a complete. Each is its own multi-session build.

### Phase 4g — paid-data scanners
After 4a + subscription decisions made. Subscribe → build → ship. 4 scanners, plan for 1-2 sessions each.

### Phases 5-7 — production readiness
Logging → alerting → deployment. Required before any real money.

### Phases 8-12 — refinements and growth
Run in parallel with paper observation period.

---

# How to resume next session

1. `cd "C:\Users\Sean Coleman\strategy_bot"`, `git pull origin main`
2. Read this file
3. Check `scan_output/` for any new scanner CSVs
4. Run `python -m pytest tests/ -q` (should be 181 passing)
5. Pick from current phase's checklist — next up is #5 fda_calendar
6. End of session: update this file's "Last updated" + checkboxes, commit

---

# Open risks / known issues

- **V3 underperforms SPY by 4 CAGR.** By design (defensive portfolio). If scanner-driven branches don't add edge, trunk allocation should be revisited (4f).
- **Single-period backtest.** 7.4% / -17.2% / 0.68 are 2021-2026 numbers. Different regimes will produce different numbers. Don't anchor.
- **No tax drag in backtests.** 277 trades over 5y is real tax events. After-tax CAGR is lower than 7.4%.
- **Quote-based ledger writes.** Paper-grade. Real money needs fill-status polling (7a).
- **VXUS/BND still in paper account.** Auto-liquidation will sell them at next executed rebalance. Need wind-down feature first (7a).
- **Spinoff tracker false-positive parents.** Filter #3 occasionally matches the wrong parent CIK on coincidental name collisions. Acceptable for v1.
- **Phase 11 fundamentally hard.** Most retail systematic strategies fail at the rules-encoding step. Treat with extreme skepticism.