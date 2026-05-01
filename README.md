# Oak Strategy Trading Bot

Phase 1 implementation: an automated rebalancer that executes the oak portfolio framework against an Alpaca paper or live brokerage account.

## What it does

The bot reads your portfolio state from Alpaca, compares it to target allocations defined in `config/strategy.yaml`, and computes the trades needed to bring it back into balance. The trunk and branches sleeves are automated; the acorns sleeve is held as cash for now and managed manually.

Two commands:

- `python main.py status` — show current holdings vs targets, drift, and sleeve breakdown
- `python main.py rebalance` — compute rebalance orders. By default this is a **dry run**: it prints what it would do without sending any orders. Pass `--execute` to actually submit them.

## Safety defaults

- Paper trading is the default. Live trading requires `ALPACA_PAPER=false` in your `.env` AND the `--execute` flag AND a typed confirmation.
- All orders go through the risk manager: per-order size cap, daily order count limit, drawdown kill switch, market-hours check.
- Default drift threshold is 5%, so the bot only rebalances when a holding has drifted meaningfully from target.
- Minimum order size of $100 prevents tiny meaningless trades.

## What this is NOT

This is Phase 1 of a longer build. It is not yet:
- Backtested (Phase 2)
- Monitored with alerts/logging beyond the console (Phase 3)
- Production-ready for serious capital (Phase 4-5)

Do not point this at meaningful live capital until Phases 2-4 are complete.

## Setup

```bash
# 1. Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Get Alpaca paper trading credentials
# Sign up at https://alpaca.markets and grab paper API keys from the dashboard.

# 4. Set up your environment
cp .env.example .env
# Edit .env and paste in your ALPACA_API_KEY and ALPACA_SECRET_KEY

# 5. Edit config/strategy.yaml to reflect your target allocations
# Defaults are placeholders — replace branches holdings with your conviction picks.
```

## First run

```bash
# See what's in the paper account
python main.py status

# Compute (but don't execute) rebalance orders
python main.py rebalance

# When ready to actually place paper-trading orders
python main.py rebalance --execute
```

## File layout

```
strategy_bot/
├── main.py                 # CLI entry point
├── requirements.txt
├── .env.example            # Credentials template (real .env is gitignored)
├── .gitignore
├── config/
│   └── strategy.yaml       # Target allocations and risk parameters
├── src/
│   ├── config.py           # Config loading + pydantic validation
│   ├── broker.py           # Alpaca client wrapper
│   ├── strategy.py         # Oak rebalancer logic
│   ├── risk.py             # Pre-execution risk checks
│   └── executor.py         # Order placement
└── tests/                  # (Phase 2)
```
