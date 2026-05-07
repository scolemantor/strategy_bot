"""Default bridge initialization shared by scan.py, meta_ranker, watchlist.

Call init_default_bridge() at the top of any entry point that fires alerts.
Idempotent — second call is a no-op (checks bridge.is_initialized()).

Reads LOG_DIR env (default 'logs'). Constructs JsonLinesLogger with
auto_rotate=True. Tries PushoverDispatcher with config/alerting.yaml; if
that fails (missing creds, bad config, etc.) the bridge runs logger-only
and prints a stderr warning. Pushover failures must NEVER prevent the
scanner pipeline from running.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

from . import bridge


def init_default_bridge(
    log_dir: Optional[Path] = None,
    pushover_config: Optional[Path] = None,
) -> None:
    if bridge.is_initialized():
        return

    if log_dir is None:
        log_dir = Path(os.environ.get("LOG_DIR", "logs"))
    if pushover_config is None:
        pushover_config = Path("config/alerting.yaml")

    # Logger first — even if Pushover fails, audit trail survives
    logger = None
    try:
        from src.logging_v2 import JsonLinesLogger
        logger = JsonLinesLogger(log_dir, auto_rotate=True)
    except Exception as e:
        sys.stderr.write(
            f"alerting.setup: JsonLinesLogger init failed ({e}); "
            "audit trail disabled\n"
        )

    dispatcher = None
    try:
        from .pushover import PushoverDispatcher
        dispatcher = PushoverDispatcher(
            config_path=pushover_config,
            logger=logger,
        )
    except Exception as e:
        sys.stderr.write(
            f"alerting.setup: PushoverDispatcher init failed ({e}); "
            "Pushover disabled\n"
        )

    email_channel = None
    try:
        from .email_channel import EmailChannel
        email_channel = EmailChannel(
            config_path=pushover_config,  # same alerting.yaml file
            logger=logger,
        )
    except Exception as e:
        sys.stderr.write(
            f"alerting.setup: EmailChannel init failed ({e}); "
            "email disabled\n"
        )

    bridge.init(dispatcher=dispatcher, logger=logger, email_channel=email_channel)
