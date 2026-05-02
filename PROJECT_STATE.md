\# strategy\_bot — Project State



Last updated: 2026-05-02



\## Current branch



`phase3` (not yet merged to `main`)



\## Phase 3 — COMPLETE



Phase 3 closed in a single session (2026-05-01 night → 2026-05-02 morning) with 11 commits.



\### What shipped



\*\*Strategy improvements\*\*

\- Vol-weighting fixes: iterative water-fill clipping (cap-then-renormalize was broken); per-column dropna (no silent symbol drops); fallback to equal weight on insufficient history rather than dropping symbols.

\- Regime overlay: `risk\_class` tagging (equity vs defensive); whipsaw protection via `buffer\_pct` and `min\_consecutive\_days` (stateless, deterministic).

\- Backtest engine: now passes sliced historical prices into the strategy on every rebalance bar (was running with regime permanently disabled due to a fetch-list bug).

\- Live rebalancer (`main.py`): fetches historical prices for vol weighting and regime; degrades gracefully on API failure.

\- Auto-liquidation: held positions outside the YAML allocation are auto-sold on next rebalance. YAML is the source of truth.



\*\*Tax-aware lot ledger (4 commits, ~2300 lines + tests)\*\*

\- `src/lot\_ledger.py`: SQLite-backed ledger with immutable `lots` table and append-only `lot\_consumptions` table. Audit trail preserved.

\- `src/tax\_lots.py`: Pure decision logic for lot selection. Priority: ST loss → LT loss → LT HIFO → ST HIFO ("minimize current-year tax burden" ordering).

\- `src/lot\_migration.py`: `seed\_from\_broker` (idempotent migration) and `reconcile\_with\_broker` (drift detection).

\- `src/executor.py`: Updates ledger on successful broker submissions. Ledger failure flags order with `\_LEDGER\_FAILED` status; doesn't stop the batch.

\- `main.py`: Auto-seeds at start of every executing rebalance, halts on reconciliation mismatch.

\- Opt-in via YAML `ledger.enabled: true`. Disabled by default — existing YAMLs run unchanged.



\*\*Portfolio retune (V3)\*\*

\- Final allocation: trunk 70 / branches 20 / acorns 10. Trunk holdings: VTI 80% / BIL 10% / GLD 10%. Branches: SMH/XLU/ITA/IHI/PAVE/INDA/COPX (inverse-vol bias 1.0).

\- 5y backtest: \*\*CAGR 7.4%, Sharpe 0.68, Max DD -17.2%.\*\* Beats SPY's implied Sharpe (~0.65) for the period; gives up CAGR for drawdown protection (SPY: 11.5% / -25.4%).

\- Three goals hit: CAGR > 7%, Sharpe > 0.65, DD < -20%.



\### Test count



181 tests passing across the suite. New test modules added:

\- `tests/test\_lot\_ledger.py` (53 tests)

\- `tests/test\_tax\_lots.py` (32 tests)

\- `tests/test\_lot\_migration.py` (27 tests)

\- `tests/test\_executor\_ledger.py` (13 tests)

\- `tests/test\_untracked\_liquidation.py` (6 tests)



\### Paper account state



\- Alpaca paper, $200k notional, seeded 2026-05-01.

\- Lot ledger initialized at `~/strategy\_bot\_data/lot\_ledger.sqlite`. 11 synthetic lots, one per held symbol, dated 2026-05-02.

\- Currently holds VXUS and BND (from old V3) — not in new YAML. Will auto-liquidate on next executing rebalance, subject to risk manager approval.



\## Open items going into Phase 4



\### Quick fixes (single-commit each)



\- \*\*`breakout\_52w` bug\*\*: Universe truncates alphabetically at A-HYR, missing MSFT/NVDA/META/TSLA. Quick patch.



\### Strategic decisions pending



\- \*\*Real-money VXUS/BND wind-down\*\*: Each is ~5% of portfolio, right at the risk manager's `max\_order\_pct\_of\_portfolio: 0.05` threshold. Single-shot liquidation will be rejected by the risk manager on real money. Need a multi-week wind-down strategy (option β: add `wind\_down: true` flag in YAML for graceful drawdown). Not blocking on paper.

\- \*\*Fill-status polling vs quote-based ledger writes\*\*: Current ledger writes use `est\_price` (latest quote) at the time orders are computed. Real fill price may differ (slippage, partial fills). For paper this is acceptable. For real money, replace with order-status polling so the ledger reflects actual fills, not requested fills. Reconciliation step catches persistent drift but doesn't auto-correct.



\### Phase 4 work (not started)



\- Per-signal historical edge tracking (Phase 4e, autonomy track per Phase 11).

\- Backtest sweep across `buffer\_pct × min\_consecutive\_days` against the new V3 baseline.

\- Scanner-driven branches (breakout, momentum, insider) — long-term path to alpha beyond static allocation.



\## V3 final YAML (config/strategy.yaml)



```yaml

allocation:

&nbsp; trunk:

&nbsp;   weight: 0.70

&nbsp;   weighting\_method: equal

&nbsp;   holdings:

&nbsp;     VTI:  { weight: 0.80, risk\_class: equity }

&nbsp;     BIL:  { weight: 0.10, risk\_class: defensive }

&nbsp;     GLD:  { weight: 0.10, risk\_class: defensive }

&nbsp; branches:

&nbsp;   weight: 0.20

&nbsp;   weighting\_method: inverse\_volatility

&nbsp;   holdings:

&nbsp;     SMH: 1.0

&nbsp;     XLU: 1.0

&nbsp;     ITA: 1.0

&nbsp;     IHI: 1.0

&nbsp;     PAVE: 1.0

&nbsp;     INDA: 1.0

&nbsp;     COPX: 1.0

&nbsp; acorns:

&nbsp;   weight: 0.10

regime:

&nbsp; enabled: true

&nbsp; benchmark: SPY

&nbsp; ma\_window: 200

&nbsp; offsignal\_cash\_pct: 0.40

&nbsp; buffer\_pct: 0.02

&nbsp; min\_consecutive\_days: 3

ledger:

&nbsp; enabled: true

&nbsp; db\_path: "~/strategy\_bot\_data/lot\_ledger.sqlite"

```



\## Architecture quick reference



\- Entry points: `backtest.py` (CLI for backtesting), `main.py` (CLI for live trading).

\- `src/strategy.py`: Pure functions for target computation, regime evaluation, vol weighting, order generation.

\- `src/backtest.py`: Backtest engine. Slices history per bar, calls strategy, simulates fills with slippage.

\- `src/broker.py`: Alpaca SDK wrapper. Returns plain dataclasses; the rest of the code never imports from `alpaca.\*`.

\- `src/data.py`: Historical bar fetcher with parquet cache at `data\_cache/`.

\- `src/executor.py`: Routes orders to broker, optionally updates ledger.

\- `src/risk.py`: Pre-trade kill switches (max order size, market hours, drawdown).

\- `src/lot\_ledger.py` / `src/tax\_lots.py` / `src/lot\_migration.py`: Tax lot tracking.

\- `src/config.py`: Pydantic models for YAML config + `.env` credentials.



