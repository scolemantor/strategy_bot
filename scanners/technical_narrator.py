"""Plain-language narration of a technical breakdown via Claude API.

Phase 8c Issue 3. Wraps a single Anthropic API call that turns the
indicator JSON dict from technical_overlay._extract_metrics() into a
2-3 paragraph trader-facing analysis.

Three layers of graceful degradation:
  1. anthropic SDK not installed -> generate_narrative() returns None
  2. ANTHROPIC_API_KEY env var not set -> returns None
  3. API call raises (network, rate limit, malformed response, etc) ->
     logs warning, returns None

The caller (technical_overlay) treats None as "no narrative available"
and stores it as null in the per-ticker JSON. Frontend then falls back
to the existing shorthand `reason` field.

Skip narration entirely if data_sufficiency != "full" — partial data
(e.g. recent IPO with <200 bars) doesn't have enough trend context to
produce a useful narrative.

Cost: ~$0.001/call (claude-haiku-4-5). Sean's projected budget is
$15-30/month for 4-10 watchlist tickers polled every 15 min during
market hours.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

log = logging.getLogger(__name__)

try:
    from anthropic import Anthropic
    SDK_AVAILABLE = True
except Exception as _imp_err:
    Anthropic = None
    SDK_AVAILABLE = False
    _SDK_IMPORT_ERROR = str(_imp_err)

ANTHROPIC_API_KEY_ENV = "ANTHROPIC_API_KEY"
NARRATOR_MODEL = "claude-haiku-4-5-20251001"
NARRATOR_MAX_TOKENS = 600
NARRATOR_TIMEOUT_SECONDS = 20  # don't block scan loop on slow API

SYSTEM_PROMPT = (
    "You are a professional technical analyst writing for an experienced "
    "retail trader. Given the indicator data below, write a 2-3 paragraph "
    "plain-language analysis covering: (1) current trend status with "
    "specific numbers, (2) momentum and volume confirmation, (3) key "
    "levels and recommended action including suggested entry zones and "
    "stop loss. Be specific and actionable. Address the trader directly. "
    "No fluff."
)


def generate_narrative(
    technical_dict: dict,
    ticker: str,
) -> Optional[str]:
    """Generate a 2-3 paragraph plain-language analysis of a technical
    breakdown. Returns None on any failure (caller treats None as
    "no narrative available")."""
    if not SDK_AVAILABLE:
        log.debug(f"  {ticker}: anthropic SDK not installed; skipping narration")
        return None

    api_key = os.environ.get(ANTHROPIC_API_KEY_ENV)
    if not api_key:
        log.debug(
            f"  {ticker}: {ANTHROPIC_API_KEY_ENV} not set; skipping narration"
        )
        return None

    if technical_dict.get("data_sufficiency") != "full":
        log.debug(
            f"  {ticker}: data_sufficiency={technical_dict.get('data_sufficiency')!r} "
            f"!= 'full'; skipping narration"
        )
        return None

    # Compact payload — drop fields the LLM doesn't need (bar_count,
    # computed_at) and keep only the analytical metrics.
    payload = {
        "ticker": ticker,
        "last_close": technical_dict.get("last_close"),
        "setup_score": technical_dict.get("setup_score"),
        "trend": technical_dict.get("trend"),
        "momentum": technical_dict.get("momentum"),
        "volume": technical_dict.get("volume"),
        "volatility": technical_dict.get("volatility"),
        "key_levels": technical_dict.get("key_levels"),
    }

    try:
        client = Anthropic(api_key=api_key, timeout=NARRATOR_TIMEOUT_SECONDS)
        message = client.messages.create(
            model=NARRATOR_MODEL,
            max_tokens=NARRATOR_MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": json.dumps(payload, indent=2, default=str),
                },
            ],
        )
        # message.content is a list of content blocks. For a plain-text
        # response (no tool use), there's typically one TextBlock.
        parts = []
        for block in message.content:
            text = getattr(block, "text", None)
            if text:
                parts.append(text)
        narrative = "".join(parts).strip()
        if not narrative:
            log.warning(f"  {ticker}: narrator returned empty text")
            return None
        return narrative
    except Exception as e:
        log.warning(
            f"  {ticker}: narrator call failed ({type(e).__name__}: {e}); "
            f"falling back to None"
        )
        return None
