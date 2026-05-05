# strategy_bot — Project State

> Living document. Update at the end of every working session.

**Last updated:** 2026-05-04 (late evening, post-Phase-4d-complete)
**Repo:** github.com/scolemantor/strategy_bot
**Account:** Alpaca paper, $200k notional, seeded 2026-05-01 11:57am ET

---

## TL;DR

- **Phases 1, 2, 3:** DONE. Phase 3 merged 2026-05-02.
- **Phase 4a:** COMPLETE. 13 of 13 free-data scanners shipped 2026-05-03.
- **Phase 4b:** COMPLETE. Investability filter all 7 gates working 2026-05-04.
- **Phase 4c:** COMPLETE. Cross-scanner meta-ranker shipped 2026-05-04.
- **Phase 4d:** COMPLETE. Watchlist tracker shipped 2026-05-04.
- **Phase 4g:** 4 paid-data scanners (#14-17) deferred pending subscription decisions.
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

**Daily scanner workflow:**
- `python scan.py all` — run all 13 scanners with investability filter
- `python -m scanners.meta_ranker --date YYYY-MM-DD` — cross-validation ranking
- `python scan.py watch digest --date YYYY-MM-DD` — personal watchlist deltas
- Three glanceable outputs: `master_ranked.csv`, `conflicts.csv`, `watchlist_digest.csv`

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

**Full 17-scanner build is locked. No scanner is being skipped, ever.** Free-data scanners (4a) complete. Paid-data scanners (4g) ship after subscription decisions.

### 4a — Free-data scanners (13 of 13 SHIPPED 2026-05-03)

- `[x]` **#1 insider_buying** — SEC Form 4 cluster buys (2+ insiders, $5K min). 19 candidates first run (CHTR, ABT, GEHC, WASH, AVLN). XSL-stripping fix applied.
- `[x]` **#2 breakout_52w** — Alpaca bars, new 52w highs on above-avg volume. 155 candidates (TWLO, NVT, MXL, BAND). Universe truncation, batching, lookback gates, sanity caps fixed.
- `[x]` **#3 earnings_drift** — yfinance, post-earnings drift on big beats. 20+ candidates (CIEN +82%, MU +22%, HPE +36%). S&P 500 universe + parquet cache + sanity caps.
- `[x]` **#4 spinoff_tracker** — SEC Form 10/10-12B/10-12G. 14 candidates after 3 filters (name patterns, CIK age, parent 8-K cross-reference). FDXF/HONA/Enviri II all surfaced.
- `[x]` **#5 fda_calendar** — RTTNews scrape for small/mid biotech PDUFA decisions in 30-90 day window. 8 setups (ARVN, SPRO, ACHV, UNCY).
- `[x]` **#6 thirteen_f_changes** — SEC Form 13F-HR for 22 curated smart-money funds (Berkshire, Pershing Square, Lone Pine, Coatue, Tiger Global, etc) + OpenFIGI CUSIP→ticker. 20 conviction signals: Pershing META 1.76B new, Pershing AMZN +65%, Coatue NFLX/AMAT/DASH adds, Lone Pine 7-name conviction list. Citadel/Millennium/Two Sigma removed (market-makers, not signals).
- `[x]` **#7 short_squeeze** — FINRA short interest + yfinance float + Alpaca momentum. 14 candidates: WOLF +110% active squeeze, DBI 12.4 DTC, SG, SPHR, KSS, NMAX. ACORNS SLEEVE ONLY.
- `[x]` **#8 small_cap_value** — yfinance fundamentals + S&P 1500 universe. P/E<12, P/B<1.5, EV/EBITDA<8, D/E<1, FCF>0, mcap $300M-3B. 3 candidates (VRTS, AMPH, INVA) — tight by design in late-cycle market.
- `[x]` **#9 sector_rotation** — Alpaca SPDR ETFs (XLF/XLK/XLE/XLV/XLI/XLP/XLY/XLU/XLB/XLRE/XLC) vs SPY 1m/3m relative strength. 1 candidate XLK Technology +10% RS 1m, accelerating. (6m signal dropped — Alpaca unadjusted prices unreliable for ETFs over long lookback.)
- `[x]` **#10 earnings_calendar** — yfinance Ticker.calendar + earnings_dates + Alpaca bars for historical 3-day post-earnings move. 361 candidates reporting next 5 trading days, ranked by avg ±% magnitude. Top picks: SEZL ±35%, LUMN ±27%, APP ±23%, PLTR ±13% reporting tomorrow. PRE-EARNINGS RISK AWARENESS, not directional.
- `[x]` **#11 macro_calendar** — pure date math (zero APIs). FOMC dates hardcoded from federalreserve.gov, BLS releases computed from monthly patterns (NFP=1st Fri, CPI=2nd Wed, PPI=day before CPI, GDP=last Thu Jan/Apr/Jul/Oct, Jobless Claims=every Thursday). 4 events surfaced for May 3-17: PPI May 12, CPI May 13 (the actionable one), Initial Jobless Claims May 7+14. Most reliable scanner in suite.
- `[x]` **#12 ipo_lockup** — stockanalysis.com scrape. 467 IPOs from 2025+2026 → 9 final candidates after SPAC filter + price + return-since-IPO filters. 180-day lockup expiring 0-60 days. Real wins: BETA Technologies eVTOL -52% lockup TODAY, AERO Aeromexico -22% lockup Tuesday, WLTH Wealthfront -23% June 10. ACORNS SLEEVE ONLY.
- `[x]` **#13 insider_selling_clusters** — SEC Form 4 cluster sells (2+ insiders, $1M+ aggregate). Subclasses scanner #1 to reuse 100% of plumbing. 62 candidates: TXN 12 insiders $113.7M (extraordinary), CRWV 6 insiders $2.9 BILLION (post-IPO lockup dump confirming #12 thesis), ELF 6 insiders $20M, URI 4 insiders $51M, UTHR 3 insiders $113M.

### 4b — Investability filter (DONE 2026-05-04)

Universal quality gate every scanner result passes through. Lives in `scanners/investability.py` + `scanners/sec_fundamentals.py`. Per-scanner config in `config/investability.yaml`. Hard-exclusions list in `config/exclusions.yaml`.
- `[x]` Market cap floor (configurable: $300M / $50M / $10M tiers, off for ETF/event scanners)
- `[x]` Average daily dollar volume minimum (computed from cached Alpaca bars)
- `[x]` Recent dilution detector (90-day shares outstanding diff from SEC 10-Q filings)
- `[x]` Going-concern flag from latest 10-K (regex on filing text for "substantial doubt" language)
- `[x]` Listing exchange filter (OTC dropped via yfinance exchange field)
- `[x]` Hard exclusions list (manually maintained, currently empty template)
- `[x]` Filtered-out audit trail (per-scanner `<scanner>_rejected.csv` with rejection_reason column)

Validated 2026-05-04 sweep across all 13 scanners: filter cascade does real work everywhere. Going-concern detection caught WLACW SPAC warrant (substantial doubt + mcap unavailable + ADV $0.61M). 3 bugs fixed mid-build (YAML 'off' coercion to False, _load_exclusions None handling, yfinance fundamentals universe coverage gap).

### 4c — Cross-scanner meta-ranker (DONE 2026-05-04)

Lives in `scanners/meta_ranker.py`. Per-scanner config in `config/scanner_weights.yaml`. CLI: `python -m scanners.meta_ranker --date YYYY-MM-DD`.
- `[x]` Aggregator (loads all 13 scanner CSVs from a date dir, builds master DataFrame)
- `[x]` Signal vector per ticker (per-scanner score + direction + category)
- `[x]` Composite scoring (normalized score * weight * multi-scanner bonus * category diversity bonus)
- `[x]` `scan_output/<date>/master_ranked.csv` output
- `[x]` Configurable signal weights (`config/scanner_weights.yaml`)
- `[x]` Conflict detection (`conflicts.csv` for tickers hit by both bullish AND bearish scanners — bullish_sum - bearish_sum + neutral_sum)
- `[x]` Per-scanner contribution audit (`category_summary.csv`)

Three v2 fixes applied: (1) p90 score normalization so multi-scanner overlap dominates raw magnitude; (2) same-scanner duplicate dedupe via groupby+sum; (3) earnings_calendar dropped when single-scanner. Validated 2026-05-03: 608 raw -> 265 post-filter unique tickers in master, top results show real cross-validation (NOW/HPE/VRT/GNRC = earnings_drift+thirteen_f_changes; MTZ = breakout_52w+thirteen_f_changes), conflicts correctly surface CRWV at -1.48 net.

### 4d — Watchlist tracker (DONE 2026-05-04)

Lives in `scanners/watchlist.py`. Storage in `config/watchlist.yaml`. CLI: `python scan.py watch add/remove/list/digest`.
- `[x]` Watchlist storage (`config/watchlist.yaml` with per-ticker added_date/reason/category)
- `[x]` CLI: `python scan.py watch add/remove/list TICKER`
- `[x]` Daily watchlist run with delta detection (NEW / DROPPED / STRONGER / WEAKER / SAME flags)
- `[x]` Watchlist-only digest separate from main scan results (`watchlist_digest.csv`)
- `[x]` Auto-removal rules — implemented as STALE flag (no scanner hits in 14+ days) rather than silent auto-remove, preserves long-term theses with sparse signal

Validated 2026-05-04 against 2026-05-03 scanner output: 4 test tickers (BLLN, CRWV, HPE, NOW), 7 digest rows produced, all delta types working. CRWV correctly shows BOTH thirteen_f_changes (ARK +311% accumulation $137.8M) AND insider_selling_clusters ($2.9B dump) — surfacing WHICH fund is on the other side of the conflict. HPE earnings_drift flagged STRONGER (rising score = momentum building).

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

Subscription decisions to be made with 4a-f complete and clear view of which signals matter most.

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
- `[ ]` Scanner #9 sector_rotation feeds into branch overweighting decisions here

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

### Phase 4a — DONE
13 scanners shipped 2026-05-03 in single multi-session push.

### Phase 4b — DONE
Investability filter (all 7 gates) shipped 2026-05-04. Universal quality gate now active on all scanner outputs.

### Phase 4c — DONE
Cross-scanner meta-ranker shipped 2026-05-04. Daily morning output is now `scan_output/<date>/master_ranked.csv` with cross-scanner conviction scoring + `conflicts.csv` flagging bullish-bearish overlaps.

### Phase 4d — DONE
Watchlist tracker shipped 2026-05-04. Daily workflow now: scan.py all -> meta_ranker -> watch digest. Three glanceable outputs per day: master_ranked.csv (cross-scanner), conflicts.csv (bullish+bearish), watchlist_digest.csv (personal tracking with deltas).

### Phase 4e/f — supporting infrastructure
Now unblocked. Locked sequence: 4e (backtested signal weighting — REQUIRED for Phase 11 autonomy) → 4f (Phase 3 retroactive items).

### Phase 4g — paid-data scanners
After 4e-f complete and subscription decisions made. 4 scanners, plan for 1-2 sessions each.

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
5. Pick from current phase's checklist — 4a/4b/4c/4d complete, ready for 4e (backtested signal weighting)
6. End of session: update this file's "Last updated" + checkboxes, commit

---

# Open risks / known issues

- **V3 underperforms SPY by 4 CAGR.** By design (defensive portfolio). If scanner-driven branches don't add edge, trunk allocation should be revisited (4f).
- **Single-period backtest.** 7.4% / -17.2% / 0.68 are 2021-2026 numbers. Different regimes will produce different numbers. Don't anchor.
- **No tax drag in backtests.** 277 trades over 5y is real tax events. After-tax CAGR is lower than 7.4%.
- **Quote-based ledger writes.** Paper-grade. Real money needs fill-status polling (7a).
- **VXUS/BND still in paper account.** Auto-liquidation will sell them at next executed rebalance. Need wind-down feature first (7a).
- **Spinoff tracker false-positive parents.** Filter #3 occasionally matches the wrong parent CIK on coincidental name collisions. Acceptable for v1.
- **Sector rotation 6m signal unreliable.** Alpaca returns unadjusted prices for some ETFs; 6m returns nonsensical when distributions/splits in window. Dropped from scoring; output kept for diagnostic only.
- **Macro calendar needs 2027 FOMC dates.** Currently only 2026 hardcoded. Update when Fed publishes 2027 schedule (typically late 2026).
- **IPO lockup assumes 180-day convention.** Real lockup terms vary (90/180/365 day). Doesn't read S-1 filings to verify; doesn't check for early-release waivers.
- **Insider selling can't filter 10b5-1 plans.** Parsed Form 4 data doesn't extract the plan flag. Cluster requirement (2+ insiders) + $1M aggregate filter most routine sales.
- **Investability filter shares-outstanding regex is approximate.** Extracts from 10-K/10-Q cover-page text via regex; may miss some filings with non-standard formatting. Dilution detection works for the common cases; misses are silent (returns None, doesn't reject).
- **Going-concern detection is text-pattern based.** Catches the standard SEC-required language but a creative auditor could phrase it differently. False negatives are possible; false positives unlikely (the specific phrases are rare in non-going-concern contexts).
- **Meta-ranker scanner_weights.yaml requires periodic recalibration.** Weights are currently judgment-based (smart-money scanners 1.2-1.3, technical 1.0, speculative 0.7). Phase 4e will replace with backtest-derived weights. Until then, the rankings are directionally right but not optimally tuned.
- **Watchlist digest only walks back 19 days for STALE detection.** Tickers added more than 19 days ago that have NEVER appeared in any scanner will simply show STALE without an actual last_seen date. Acceptable for v1 — long-term staleness is the point of the flag anyway.
- **Phase 11 fundamentally hard.** Most retail systematic strategies fail at the rules-encoding step. Treat with extreme skepticism.
- **AI assistant order-of-build tendency.** Sean noted 2026-05-03: assistant has a habit of wanting to skip to interesting infrastructure work (4c meta-ranker, 4b filters, etc) before completing the locked sequential build. Sean's correction: WE DO NOT STOP. WE FINISH WHAT WE STARTED IN ORDER UNTIL IT IS COMPLETE AND THEN WE MOVE ON. Sequence is non-negotiable: 4a → 4b → 4c → 4d → 4e → 4f → 4g → Phase 5 → onward. Assistant should not propose reordering without explicit Sean approval. This applies to scanner builds, infrastructure work, and any future phase decisions.