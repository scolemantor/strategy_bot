# strategy_bot — Project State

> Living document. Update at the end of every working session.

**Last updated:** 2026-05-06 (early morning, Phase 4e year backtest in progress)
**Repo:** github.com/scolemantor/strategy_bot
**Account:** Alpaca paper, $200k notional, seeded 2026-05-01 11:57am ET

---

## TL;DR

- **Phases 1, 2, 3:** DONE.
- **Phase 4a:** COMPLETE for 13 of 14 free-data scanners (shipped 2026-05-03). Scanner #18 (congressional_trades) added retroactively 2026-05-06, to build after Phase 4e finishes.
- **Phase 4b:** COMPLETE. Investability filter all 7 gates working 2026-05-04.
- **Phase 4c:** COMPLETE. Cross-scanner meta-ranker shipped 2026-05-04.
- **Phase 4d:** COMPLETE. Watchlist tracker shipped 2026-05-04.
- **Phase 4e:** IN PROGRESS. Pipeline-level backtest framework built 2026-05-05/06; year backtest running overnight 2026-05-06 → expected ~12-16 hours.
- **Phase 4g:** 4 paid-data scanners (#14-17) deferred pending subscription decisions.
- **Phases 5-15:** not started. End goal: production-deployed bot with polished iPhone app via TestFlight.
- **181 tests passing.**
- **V3 portfolio:** 7.4% CAGR, -17.2% max DD, Sharpe 0.68 over 5y. Beats SPY's risk-adjusted return.

---

## North Star

Build a system that runs autonomously, has measurable edge, and is interfaced through a professional iPhone app distributed via TestFlight. Personal use first; public productization is an option but not required for success.

---

## Current state of the bot

**Portfolio V3 (locked in YAML):**
- Trunk 70%: VTI 80% / BIL 10% / GLD 10%
- Branches 20%: SMH, XLU, ITA, IHI, PAVE, INDA, COPX (inverse-vol weighted)
- Acorns 10%: cash, manually managed (highest-conviction sleeve)

**Live behavior:**
- Vol weighting on branches with iterative water-fill clipping (5%/40% bounds, 90-day window)
- Regime detection on SPY 200dma with 2% buffer + 3 consecutive days of confirmation
- Defensive tagging: BIL and GLD held flat during regime-off
- Auto-liquidation of held positions outside YAML allocation
- Tax-aware lot ledger (HIFO + long-term preference + losses-first)

**Daily scanner workflow:**
- `python scan.py all` — 13 scanners with investability filter
- `python -m scanners.meta_ranker --date YYYY-MM-DD` — cross-validation ranking
- `python scan.py watch digest --date YYYY-MM-DD` — personal watchlist deltas
- Three glanceable outputs: `master_ranked.csv`, `conflicts.csv`, `watchlist_digest.csv`

**Backtest infrastructure (Phase 4e):**
- 9 of 13 scanners have `backtest_mode(as_of_date)` (small_cap_value is stub — no historical fundamentals)
- Pipeline replay aggregates scanners → meta-ranker → top-N basket → forward returns
- Edge metrics computed under multiple win definitions (any beat / material / strong / absolute / after-costs)

**Paper account positions (as of 2026-05-01):** Still holds VXUS and BND from V1 seeding. Auto-liquidation will sell them at next executed rebalance. Wind-down logic needed before live (Phase 7a).

---

## Cross-machine workflow

- **Code:** GitHub. Pull at session start, commit + push at session end.
- **Cache:** `data_cache/` symlinked to `G:\My Drive\strategy_bot_cache\data_cache\` on both machines.
- **Scan output:** `scan_output/` symlinked to `G:\My Drive\strategy_bot_cache\scan_output\` on both machines.
- **Backtest output:** `backtest_output/` symlinked to `G:\My Drive\strategy_bot_cache\backtest_output\`.
- **`.env`:** machine-local, never synced. Paper keys regenerated 2026-05-01 after accidental exposure.

---

# PHASE PLAN — full detail

## ✅ Phase 1: Live Skeleton — DONE

CLI rebalancer with Alpaca integration. 11 positions seeded paper account 2026-05-01 at 0% drift.

## ✅ Phase 2: Backtest Harness — DONE

Historical bar fetcher with parquet caching, backtest engine with 5bps slippage, CAGR/Sharpe/DD stats, benchmark comparison.

## ✅ Phase 3: Strategy Enhancements — DONE

Vol-weighted branches, regime detection, defensive tagging, V3 retune, tax-aware lot ledger, auto-liquidation.

## 🔄 Phase 4: Acorns Idea-Generation Scanner — IN PROGRESS

**Full 17-scanner build is locked. No scanner is being skipped.**

### 4a — Free-data scanners (13 of 14 SHIPPED 2026-05-03; #18 retroactively added)

13 scanners shipped 2026-05-03: insider_buying, breakout_52w, earnings_drift, spinoff_tracker, fda_calendar, thirteen_f_changes, short_squeeze, small_cap_value, sector_rotation, earnings_calendar, macro_calendar, ipo_lockup, insider_selling_clusters.

**Scanner #18 — congressional_trades (retroactive add, build after 4e):**
- Category: conviction
- Direction: bullish (mostly — buys outnumber sells; cluster sells flagged separately)
- Weight: 1.1 (between insider_buying 1.2 and thirteen_f_changes 1.3)
- Data source: housestockwatcher.com + senatestockwatcher.com community APIs (parses official STOCK Act PTR PDFs)
- Logic: flag tickers bought by 2+ members within 30-day window, OR by high-signal individual members (track list configurable in YAML) above $50k transaction size
- Backtest mode: yes — disclosures are date-stamped and immutable; filing dates lag transactions by up to 45 days but the lag is consistent so look-ahead protection is straightforward (use disclosure_date <= as_of_date, not transaction_date)
- Known issues: 45-day disclosure lag means signal is stale by definition; some members file late or amend after deadlines; spouse/family member trades disclosed but harder to attribute; PDF parsing edge cases occasionally produce malformed entries in community feeds
- Why it was missed initially: oversight when the original 17-scanner spec was drafted. Free data, documented edge (Ziobrowski 2011 + follow-on academic work), fits existing conviction-category architecture cleanly. Should have been #14 from day 1.

### 4b — Investability filter (DONE 2026-05-04)

All 7 gates: mcap floor, ADV floor, dilution detector, going-concern flag, exchange filter, hard exclusions, audit trail.

### 4c — Cross-scanner meta-ranker (DONE 2026-05-04)

Aggregates all 13 scanner CSVs into master_ranked.csv with cross-validation scoring, conflict detection, category diversity bonus.

### 4d — Watchlist tracker (DONE 2026-05-04)

Add/remove/list/digest CLI. Daily delta detection (NEW/DROPPED/STRONGER/WEAKER/SAME). Stale flag at 14+ days.

### 4e — Backtested signal weighting (IN PROGRESS — year backtest running)

Pipeline-level backtest infrastructure built 2026-05-05/06. Replays full system (scanners → meta-ranker) historically and measures top-5/10/20 basket forward returns vs SPY at 5/21/63 day horizons.
- `[x]` `scanners/backtest/forward_returns.py` — N-day excess return calculator
- `[x]` `scanners/backtest/edge_metrics.py` — multiple win definitions, win/loss ratio, Sharpe
- `[x]` `scanners/backtest/pipeline_replay.py` — orchestrator
- `[x]` `backtest_mode()` added to 8 scanners (breakout_52w, insider_buying, insider_selling_clusters, earnings_drift, thirteen_f_changes, spinoff_tracker, short_squeeze, sector_rotation)
- `[x]` `small_cap_value` stub (no historical fundamentals available)
- `[ ]` Year backtest result analysis (running overnight)
- `[ ]` Recalibrate `scanner_weights.yaml` based on measured edge
- `[ ]` Add drift detection (signals that decay over time)
- `[ ]` Output: per-scanner edge report with confidence intervals (v2 enhancement)

Cache merge bug fixed in `src/data.py` 2026-05-05. SEC filings cache compounds across replay dates so each subsequent date is faster.

### 4f — Phase 3 retroactive items

- `[ ]` Regime params backtest sweep: try (buffer, days) combinations of (0.01, 2), (0.02, 3), (0.03, 5), (0.05, 5).
- `[ ]` Trunk allocation re-evaluation after scanners produce alpha.

### 4g — Paid-data scanners (deferred — all four ship)

- `[ ]` **#14 options_unusual** — Unusual Whales / FlowAlgo
- `[ ]` **#15 crypto_onchain** — Glassnode / CoinMetrics
- `[ ]` **#16 sentiment** — Reddit + Google Trends + StockTwits unified pipeline
- `[ ]` **#17 ma_rumors** — Bloomberg / Reuters / Benzinga news API

## ⬜ Phase 5: Persistent Logging

Foundation for everything downstream including the iOS app.
- `[ ]` Structured JSON Lines logging to disk
- `[ ]` Order audit log (never auto-rotated)
- `[ ]` Scanner output history archived
- `[ ]` Rebalance history with proposed/executed orders, drift, regime
- `[ ]` Log rotation (daily, gzipped after 7d, deleted after 90d)
- `[ ]` Log query CLI
- `[ ]` Critical events stay forever
- `[ ]` Sensitive fields redacted

## ⬜ Phase 6: Alerting & Notifications

Required for Phase 7 deployment. iOS app push notifications come later in Phase 13 — Phase 6 ships email + Slack first.
- `[ ]` Notification channel config
- `[ ]` Critical alerts: kill switch, drawdown breach, order failure, auth failure, exception
- `[ ]` Operational alerts: daily summary, rebalance occurred, scanner finished, watchlist signal
- `[ ]` Daily digest format
- `[ ]` Slack integration via webhook
- `[ ]` Email integration via SMTP
- `[ ]` Quiet hours
- `[ ]` Rate limiting + dedup
- `[ ]` Test mode

## ⬜ Phase 7: Production Deployment

- `[ ]` Persistent state store (drawdown HWM, last-run timestamps, request IDs)
- `[ ]` Dockerfile (one-command deploy)
- `[ ]` Cloud VM provisioning (Hetzner or DigitalOcean ~$5-10/mo)
- `[ ]` Cron schedule (status daily, rebalance weekly, scanners daily)
- `[ ]` Health check endpoint / dead-man switch

### 7a — Pre-live retroactive items

- `[ ]` Fill-status polling (replace quote-based ledger writes)
- `[ ]` VXUS/BND wind-down logic

### 7b — Pre-live gates

- `[ ]` 30-day paper observation period (no crashes, no missed rebalances)
- `[ ]` Real-money go-live with $5-10k slice first
- `[ ]` 30+ clean days at small size
- `[ ]` Scale to full $200k

## ⬜ Phase 7.5 (NEW): API Layer — REQUIRED FOR iOS APP

Bridge between bot and any UI. The iOS app reads from this API exclusively.
- `[ ]` FastAPI service running alongside the bot on production VM
- `[ ]` Postgres database (replacing SQLite for concurrent reads)
- `[ ]` Redis caching layer for fast API responses
- `[ ]` REST endpoints:
  - `GET /api/today/master_ranked` — top picks for today
  - `GET /api/today/conflicts` — bullish-bearish overlap names
  - `GET /api/today/watchlist_digest` — personal tracking deltas
  - `GET /api/portfolio/state` — V3 holdings, drift, regime
  - `GET /api/portfolio/lots` — tax-lot detail
  - `GET /api/scan/history?since=...&ticker=...` — historical scan archive
  - `GET /api/edge/metrics` — Phase 4e edge measurement dashboard
  - `GET /api/ticker/{symbol}` — full detail: which scanners flagged, reasons, chart data, forward-return prediction
  - `POST /api/watchlist/add`, `DELETE /api/watchlist/{ticker}` — manage watchlist
  - `GET /api/health` — for app status indicator
- `[ ]` Authentication: Apple Sign-In integration (paves way for iOS app login)
- `[ ]` Rate limiting per token
- `[ ]` API documentation (OpenAPI / Swagger auto-generated by FastAPI)
- `[ ]` Versioning strategy (`/api/v1/...` so future breaking changes don't kill old app builds)

Realistic: 2-3 weeks of focused work.

## ⬜ Phase 8: Operational Hardening

- `[ ]` Test failure modes deliberately (Alpaca down, bad quote, broker reject, VM reboot, network partition, disk full)
- `[ ]` Manual override interface (pause, force-rebalance, veto orders, read-only mode)
- `[ ]` Multi-account support (taxable + IRA + Roth)
- `[ ]` Tax-loss harvesting: wash-sale tracking, substitute-asset swap pairs, opportunistic harvest

## ⬜ Phase 9: Branches Signal Overlay

Momentum filter on conviction holdings.
- `[ ]` Signal definitions, per-branch overlay logic, three response modes
- `[ ]` Backtest across regimes
- `[ ]` Configuration: `config/branch_signals.yaml`
- `[ ]` Override flag for buy-the-dip moments
- `[ ]` Scanner #9 sector_rotation feeds into branch overweighting

## ⬜ Phase 10: Portfolio-Level Vol Targeting

Probably IRA-only due to tax friction.
- `[ ]` Vol measure choice, target vol setting, scaling logic, update cadence
- `[ ]` Backtest comparison vs static V3
- `[ ]` Interaction rules with regime detector

## ⬜ Phase 11: Acorns Sleeve Automation — High Research Bar

Highest-risk feature. Build manual workflow first, run for 6+ months, encode judgment into rules only after seeing what works.
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

## ⬜ Phase 13 (NEW): iOS App v1 — Read-Only

Native iPhone app distributed via TestFlight. SwiftUI. Read-only — shows scanner output, watchlist, portfolio, charts. No order placement yet.

**Tech stack:**
- SwiftUI (Apple's modern UI framework)
- Swift Charts (built-in, native charting)
- URLSession for API calls
- Combine or async/await for data flow
- KeychainServices for secure token storage
- Core Data or SwiftData for offline cache

**Screens:**
- `[ ]` Home: today's top 10 + conflicts + watchlist deltas + macro calendar strip
- `[ ]` Ticker detail: full chart, all scanners that flagged it with reasons, forward-return prediction, mini chart of post-flag performance
- `[ ]` Watchlist: add/remove tickers, see staleness, NEW/STRONGER/WEAKER deltas
- `[ ]` Portfolio: V3 holdings vs target, drift heatmap, rebalance preview
- `[ ]` Scanner health: edge metrics dashboard, last-run times, drift alerts
- `[ ]` Settings: connection status, push notification preferences, dark mode

**Infrastructure:**
- `[ ]` Apple Developer Program enrollment ($99/year)
- `[ ]` App Store Connect setup (TestFlight access)
- `[ ]` Xcode project + git repo
- `[ ]` API client layer with auth
- `[ ]` Push notification setup via APNs (replaces email/Slack alerts from Phase 6)
- `[ ]` Deep linking (tap notification → opens specific ticker detail)
- `[ ]` Offline cache for last-loaded data
- `[ ]` App icon design
- `[ ]` Launch screen design

**Out of scope for v1:**
- Order placement (Phase 14)
- Multi-account support (Phase 8)
- Tax-lot detail
- Social features

Realistic: 3-6 months if learning Swift, 2-3 months with hired help.

## ⬜ Phase 14 (NEW): iOS App v2 — Order Placement

Adding the ability to actually execute trades from the phone. Highest-risk feature — bugs lose real money.
- `[ ]` Biometric authentication (Face ID confirmation per order)
- `[ ]` Friction-rich confirmation screens (no accidental fat-finger trades)
- `[ ]` Read-only mode toggle for "just looking"
- `[ ]` Hard daily/weekly trade-count limits enforced server-side
- `[ ]` Server-side validation that mirrors the app's request
- `[ ]` Audit log of every order placed via app
- `[ ]` Position-size suggestions based on current portfolio + acorn rules
- `[ ]` Real-time fill status with push notification on completion
- `[ ]` Cancel/modify support
- `[ ]` Detailed order ticket (scanner that flagged → why → confirmation)

Realistic: 1-2 months on top of v1.

## ⬜ Phase 15 (NEW): TestFlight Distribution

Personal-use distribution via Apple's official beta channel. NOT public App Store.
- `[ ]` TestFlight build upload pipeline (Xcode → archive → App Store Connect)
- `[ ]` Internal testing setup (your Apple ID, up to 100)
- `[ ]` External testing setup (up to 10,000 invited Apple IDs if ever wanted)
- `[ ]` Apple beta app review (1-2 days, light vs full App Store review)
- `[ ]` Build refresh cadence — every 60-90 days minimum
- `[ ]` Privacy policy + terms (required even for TestFlight if collecting any user data)
- `[ ]` Crash reporting integration
- `[ ]` Analytics (optional — your own analytics, not third-party tracking)

Realistic: 1-2 weeks setup, then ongoing per-build refresh.

**Note:** If a future decision is made to go public on the App Store, that's a separate phase (would be Phase 16) including marketing assets, App Store review process, payment infrastructure, and potential RIA registration depending on monetization model.

---

# Sequencing

### Phase 4 — IN PROGRESS
4a/4b/4c/4d done. 4e year backtest running tonight. After 4e: 4f retroactive items, 4g paid scanners.

### Phases 5-6 — production prereqs
Logging then alerting. Required before production deployment.

### Phase 7 — production deployment
Cloud VM running the bot daily. 30-day paper observation gate before any real money.

### Phase 7.5 — API layer
Bridge between bot and iOS app. 2-3 weeks. Required before any UI work.

### Phases 8-12 — refinements
Run in parallel with iOS app build. Operational hardening, branch signal overlay, vol targeting, acorns automation, custom benchmarks.

### Phase 13 — iOS app v1
Native SwiftUI app, read-only. TestFlight distribution. Personal use first.

### Phase 14 — iOS app order placement
The dangerous one. Lots of testing.

### Phase 15 — TestFlight ongoing
Build refresh cadence, beta review, internal/external tester management.

### Total realistic timeline
~12-18 months from now to "polished iPhone app reading from production-deployed bot." Most projects of this scope take longer than estimate, not shorter.

---

# How to resume next session

1. `cd "C:\Users\Sean Coleman\strategy_bot"`, `git pull origin main`
2. Read this file
3. Check status of overnight backtest: `dir backtest_output\_pipeline_report_*`
4. Run `python -m pytest tests/ -q` (should be 181 passing)
5. Pick from current phase's checklist — 4a/4b/4c/4d complete, 4e in progress
6. End of session: update this file's "Last updated" + checkboxes, commit

---

# Open risks / known issues

- **V3 underperforms SPY by 4 CAGR.** By design (defensive). If scanner-driven branches don't add edge, trunk allocation should be revisited (4f).
- **Single-period backtest.** 7.4% / -17.2% / 0.68 are 2021-2026 numbers. Don't anchor.
- **No tax drag in backtests.** After-tax CAGR is lower than 7.4%.
- **Quote-based ledger writes.** Paper-grade. Real money needs fill-status polling (7a).
- **VXUS/BND still in paper account.** Need wind-down feature (7a).
- **Spinoff tracker false-positive parents.** Acceptable for v1.
- **Sector rotation 6m signal unreliable.** Dropped from scoring.
- **Macro calendar needs 2027 FOMC dates.**
- **IPO lockup assumes 180-day convention.** Real terms vary.
- **Insider selling can't filter 10b5-1 plans.** Cluster + $1M filter most routine sales.
- **Investability filter shares-outstanding regex is approximate.**
- **Going-concern detection is text-pattern based.**
- **Meta-ranker scanner_weights.yaml is judgment-based.** Phase 4e replaces with backtest-derived. New scanner #18 (congressional_trades) starts with judgment weight 1.1 until Phase 4e re-runs include it.
- **Watchlist STALE detection only walks back 19 days.** Acceptable for v1.
- **Phase 4e small_cap_value not backtestable.** No historical fundamentals available without paid data.
- **Phase 4e short_squeeze uses current yfinance float for historical short% calc.** Minor look-ahead bias.
- **Phase 11 fundamentally hard.** Most retail systematic strategies fail at the rules-encoding step.
- **iOS app phases 13-15 add 12-18 months.** Don't underestimate. SwiftUI learning curve, App Store Connect setup, push notifications, all add up. Hire help if timeline matters more than learning.
- **AI assistant order-of-build tendency.** Sean noted 2026-05-03: assistant has a habit of wanting to skip to interesting infrastructure work before completing the locked sequential build. Sean's correction: WE DO NOT STOP. WE FINISH WHAT WE STARTED IN ORDER UNTIL IT IS COMPLETE AND THEN WE MOVE ON. Sequence is non-negotiable: 4a → 4b → 4c → 4d → 4e → 4f → 4g → 5 → 6 → 7 → 7.5 → 8-12 → 13 → 14 → 15. Assistant should not propose reordering without explicit Sean approval.
- **Assistant scanner-spec completeness failure 2026-05-06.** Original 17-scanner spec missed congressional_trades despite it being free data with documented academic edge. Added retroactively as scanner #18. Lesson: when defining future scanner sets, explicitly check public smart-money signal sources before locking the spec.