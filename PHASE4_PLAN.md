# Phase 4 — Acorns Idea-Generation Scanner

This document tracks the multi-session build of all 17 scanners. Update it as work progresses.

## Project conventions

- Each scanner is a self-contained module in `scanners/`
- Every scanner inherits from `scanners.base.Scanner`
- Scanners produce a `pandas.DataFrame` with at minimum: `ticker`, `score`, `reason`
- Output goes to `scan_output/<YYYY-MM-DD>/<scanner_name>.csv`
- No scanner places orders. Ever. Output is read-only ideas.

## Build status (17 scanners)

Status legend: `[ ]` not started · `[~]` partial · `[x]` working & tested · `[B]` blocked on data

| # | Scanner | Status | Source | Notes |
|---|---|---|---|---|
| 1 | insider_buying | [~] | SEC EDGAR Form 4 | Built. Awaiting first live run. |
| 2 | breakout_52w | [ ] | Alpaca bars (already wired) | New 52w high + above-avg volume |
| 3 | earnings_drift | [ ] | yfinance | Post-earnings momentum, ~60d window |
| 4 | spinoff_tracker | [ ] | SEC EDGAR Form 10/8-K | Forced-seller asymmetry |
| 5 | fda_calendar | [ ] | FDA.gov | PDUFA dates, AdComs |
| 6 | thirteen_f_changes | [ ] | SEC EDGAR Form 13F | New positions by tracked funds |
| 7 | short_squeeze | [ ] | FINRA short interest + price/volume | High SI + rising price |
| 8 | small_cap_value | [ ] | yfinance fundamentals | P/B, FCF yield, debt screens |
| 9 | sector_rotation | [ ] | sector ETF returns | 1m / 3m / 6m relative strength |
| 10 | earnings_calendar | [ ] | yfinance | Implied move + earnings reactions |
| 11 | macro_calendar | [ ] | TradingEconomics or scrape | Fed, CPI, jobs, OPEC |
| 12 | ipo_lockup | [ ] | scraping IPO calendar | 180d lockup expiry watchlist |
| 13 | insider_selling_clusters | [ ] | SEC EDGAR Form 4 | Inverse signal — don't buy |
| 14 | options_unusual | [B] | Paid API likely | Free version is degraded |
| 15 | crypto_onchain | [B] | Glassnode (free tier limited) | Active addresses, exchange flows |
| 16 | sentiment | [B] | Reddit/Trends fragmented | Pushshift dead, Trends rate-limited |
| 17 | ma_rumors | [B] | News API needed | $50-200/mo for clean coverage |

## Per-session log

### Session 2026-05-01

- Wrote `PHASE4_PLAN.md`
- Wrote `scanners/base.py` (Scanner ABC, ScanResult dataclass)
- Wrote `scanners/edgar_client.py` (rate-limited SEC client + CIK->ticker)
- Wrote `scan.py` CLI
- Built scanner #1 (insider_buying) — full pipeline including CIK->ticker, Form 4 XML parsing, A/D filtering, cluster detection
- Pending: first live run on user's machine

### Session NEXT

- Validate scanner #1 output makes sense
- Build scanner #2 (breakout_52w) — easy, uses existing Alpaca data layer
- Build scanner #3 (earnings_drift) — requires yfinance

## Data source learnings

### SEC EDGAR

- Base: `https://www.sec.gov`
- Required header: `User-Agent: <name> <email>` (SEC requires identification)
- Rate limit: 10 requests/second per IP (we use 5/s to be polite)
- CIK->ticker map: `https://www.sec.gov/files/company_tickers.json` (refresh weekly)
- Daily index of all filings: `https://www.sec.gov/Archives/edgar/daily-index/<YYYY>/QTR<n>/form.<YYYYMMDD>.idx`
- Individual filing index: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=<cik>&type=4`
- Form 4 XML lives inside the filing's primary document