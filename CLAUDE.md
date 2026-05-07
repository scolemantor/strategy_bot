\# strategy\_bot — Claude Code project context



> This file is loaded into every Claude Code session. Keep it under 200 lines.

> The full plan lives in PROJECT\_STATE.md — read that for the comprehensive roadmap.



\## What this project is



A personal Python trading bot that runs scanners against free data sources (SEC EDGAR, FINRA, yfinance, Alpaca), aggregates signals via a meta-ranker, and feeds an "acorns sleeve" of high-conviction stock picks. Long-term goal: production deployment + native iPhone app via TestFlight.



Sean (the person you're working with) owns this project. He runs it on Windows. Email: seanpcoleman1@gmail.com (use for any git config, scripts, or examples).



\## Current status (read PROJECT\_STATE.md for full detail)



\- Phases 1-3: DONE (rebalancer, backtest harness, V3 strategy)

\- Phase 4a-4d: DONE (13 free-data scanners, investability filter, meta-ranker, watchlist tracker)

\- Phase 4e: IN PROGRESS — pipeline-level year backtest CURRENTLY RUNNING (started 2026-05-06 02:28). Do NOT kill the running python process. Estimated finish: Friday/Saturday 2026-05-08/09.

\- Phase 4f, 4g, 5-15: not started



\## Critical operational rules



1\. \*\*DO NOT KILL THE RUNNING BACKTEST.\*\* A python process is running `pipeline\_replay.py` to generate ~52 weekly replay dates from 2024-05-01 to 2025-04-30. Output lands in `backtest\_output/\_pipeline\_report\_2026-05-06/`. Killing it loses days of compute.



2\. \*\*WE DO NOT STOP. WE FINISH WHAT WE STARTED IN ORDER.\*\* Sean's directive 2026-05-03. Sequence is locked: 4a → 4b → 4c → 4d → 4e → 4f → 4g → 5 → 6 → 7 → 7.5 → 8-12 → 13 → 14 → 15. Do not propose reordering without explicit Sean approval.



3\. \*\*Paper account only.\*\* Alpaca paper trading, $200k notional. .env files contain paper API keys. Never commit .env. Never propose using real-money credentials.



4\. \*\*Scanner #18 (congressional\_trades) is queued retroactively.\*\* Build AFTER Phase 4e finishes, BEFORE 4f starts. Free data via housestockwatcher.com / senatestockwatcher.com APIs.



\## Tech stack



\- Python 3.11 in .venv

\- Key deps: alpaca-py, yfinance, pandas, lxml, requests, pyyaml, pytest, pyarrow (parquet)

\- Tests: pytest. Run `python -m pytest tests/ -q` (181 tests passing baseline)

\- No type checking enforced; codebase is loosely typed but uses type hints in newer modules



\## Code conventions in this repo



\- All scanner modules live in `scanners/`

\- Each scanner exports `run(creds, output\_dir, \*\*kwargs)` for production and `backtest\_mode(as\_of\_date, output\_dir)` for historical replay

\- Scanner CSVs go to `scan\_output/<date>/<scanner\_name>.csv` in production, `backtest\_output/<date>/<scanner\_name>.csv` in backtest mode

\- Configuration in `config/\*.yaml`. Most important: `scanner\_weights.yaml` (meta-ranker), `portfolio\_v3.yaml` (target allocation), `watchlist.yaml` (tracked tickers)

\- SEC client: `scanners/edgar\_client.py` (rate-limited 5 req/sec, User-Agent: OakStrategyBot {seanpcoleman1@gmail.com})

\- Cache layer: `scanners/sec\_cache.py` for SEC data, `src/data.py` for Alpaca bars (parquet, merge-on-write)

\- Test files: `tests/test\_<module>.py`. Use real data fixtures where possible.



\## Files NOT to read or modify casually



\- `data\_cache/` — thousands of parquet files; gitignored

\- `scan\_output/`, `backtest\_output/` — generated outputs; gitignored

\- `.env` — credentials; never commit

\- `PROJECT\_STATE.md` — Sean's living planning doc; only edit when explicitly asked, and propose changes via diff first



\## Active known issues (Phase 4e era)



\- short\_squeeze logs yfinance 404s as ERROR level; should be DEBUG. Many tickers (warrants, preferred, units, delisted) don't have yfinance data. Cosmetic noise, not failure.

\- yfinance is slower than SEC for many tickers; per-replay-date cost is ~90 minutes

\- small\_cap\_value scanner is a stub in backtest\_mode (no historical fundamentals available without paid data)

\- short\_squeeze uses CURRENT yfinance float for historical short% — minor look-ahead bias



\## Working style Sean prefers



\- Direct, blunt assessment over hedged caution

\- Engineering execution, not philosophical debates about whether to build something

\- Push back on technical concerns (real risks) but not on conviction calls (Sean's call)

\- Short responses preferred. Be concise.

\- Don't use emojis unless Sean does first

\- Sean is a "full acorn man" — concentrated, high-conviction, momentum/event-driven trader. Took paper account 100k → 994k in 3 months.



\## V3 portfolio structure



\- Trunk 70%: VTI 80% / BIL 10% / GLD 10% (defensive base)

\- Branches 20%: SMH, XLU, ITA, IHI, PAVE, INDA, COPX (inverse-vol weighted)

\- Acorns 10%: high-conviction individual stocks, manually managed (the sleeve scanners feed)



\## Common commands



```bash

\# Daily scanner workflow

python scan.py all

python -m scanners.meta\_ranker --date YYYY-MM-DD

python scan.py watch digest --date YYYY-MM-DD



\# Tests

python -m pytest tests/ -q



\# Backtest (DON'T RUN — one is already running)

\# python -m scanners.backtest.pipeline\_replay --start 2024-05-01 --end 2025-04-30

```



\## What you (Claude Code) should default to doing



\- Read PROJECT\_STATE.md for context on any phase work

\- Run pytest after any code change

\- Commit with descriptive messages including the phase number

\- Push to origin/main when work completes

\- Flag (don't auto-fix) anything that would change scanner output during a running backtest

