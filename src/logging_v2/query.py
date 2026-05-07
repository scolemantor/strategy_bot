"""CLI for querying structured logs.

python -m src.logging_v2.query search   [--since D] [--until D]
                                          [--level L] [--event-type T]
                                          [--source S] [--payload k=v]
                                          [--include-critical] [--limit N]
                                          [--format json|table]
python -m src.logging_v2.query tail     [--include-critical] [--poll-ms 500]
python -m src.logging_v2.query summary  [--since D] [--until D]
                                          [--include-critical]
python -m src.logging_v2.query maintain [--grace-days 7] [--delete-after-days 90]
"""
from __future__ import annotations

import argparse
import gzip
import json
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from .rotation import rotation_pass, JSONL_NAME_RE, JSONL_GZ_NAME_RE

DEFAULT_LOG_DIR = Path("logs")


def _open_maybe_gz(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def _file_date(path: Path) -> Optional[date]:
    for regex in (JSONL_NAME_RE, JSONL_GZ_NAME_RE):
        m = regex.match(path.name)
        if m:
            try:
                return date.fromisoformat(m.group(1))
            except ValueError:
                pass
    return None


def _iter_log_files(
    log_dir: Path,
    since: Optional[date],
    until: Optional[date],
    include_critical: bool,
) -> Iterator[Path]:
    """Yield matching log files (.jsonl + .jsonl.gz). Optionally include critical/."""
    sources = [log_dir]
    if include_critical:
        sources.append(log_dir / "critical")

    for src in sources:
        if not src.exists():
            continue
        for p in sorted(src.iterdir()):
            if not p.is_file():
                continue
            d = _file_date(p)
            if d is None:
                continue
            if since and d < since:
                continue
            if until and d > until:
                continue
            yield p


def _parse_payload_filters(payload_args: Optional[List[str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not payload_args:
        return out
    for arg in payload_args:
        if "=" not in arg:
            raise ValueError(f"--payload expects key=value, got: {arg!r}")
        k, v = arg.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _entry_matches(
    entry: Dict[str, Any],
    level: Optional[str],
    event_type: Optional[str],
    source: Optional[str],
    payload_filters: Dict[str, str],
) -> bool:
    if level and entry.get("level", "").upper() != level.upper():
        return False
    if event_type and event_type.lower() not in str(entry.get("event_type", "")).lower():
        return False
    if source and source.lower() not in str(entry.get("source", "")).lower():
        return False
    payload = entry.get("payload") or {}
    for k, v in payload_filters.items():
        if str(payload.get(k, "")) != v:
            return False
    return True


def cmd_search(args, log_dir: Path) -> List[Dict[str, Any]]:
    payload_filters = _parse_payload_filters(args.payload)
    seen_keys = set()
    matched: List[Dict[str, Any]] = []
    for path in _iter_log_files(log_dir, args.since, args.until, args.include_critical):
        with _open_maybe_gz(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not _entry_matches(
                    entry, args.level, args.event_type, args.source, payload_filters
                ):
                    continue
                if args.include_critical:
                    key = (
                        entry.get("timestamp"),
                        entry.get("event_type"),
                        entry.get("message"),
                    )
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                matched.append(entry)
                if len(matched) >= args.limit:
                    break
        if len(matched) >= args.limit:
            break

    if args.format == "table":
        for e in matched:
            print(
                f"{e.get('timestamp',''):32}  "
                f"{e.get('level','?'):7}  "
                f"{str(e.get('source','?')):30}  "
                f"{str(e.get('event_type','?')):25}  "
                f"{e.get('message','')}"
            )
    else:
        for e in matched:
            print(json.dumps(e, default=str))
    return matched


def cmd_tail(args, log_dir: Path) -> None:
    """Follow today's log file (and optionally critical's). Polls every poll_ms."""
    now = datetime.now(timezone.utc)
    today = now.date()
    paths = [log_dir / f"strategy_bot_{today.isoformat()}.jsonl"]
    if args.include_critical:
        paths.append(log_dir / "critical" / f"strategy_bot_{today.isoformat()}.jsonl")
    positions: Dict[Path, int] = {}
    for p in paths:
        positions[p] = p.stat().st_size if p.exists() else 0

    try:
        while True:
            for p in paths:
                if not p.exists():
                    continue
                size = p.stat().st_size
                if size > positions[p]:
                    with open(p, "r", encoding="utf-8") as f:
                        f.seek(positions[p])
                        for line in f:
                            line = line.strip()
                            if line:
                                print(line)
                    positions[p] = size
            time.sleep(args.poll_ms / 1000.0)
    except KeyboardInterrupt:
        pass


def cmd_summary(args, log_dir: Path) -> Dict[str, int]:
    counts: Counter = Counter()
    seen_keys = set()
    for path in _iter_log_files(log_dir, args.since, args.until, args.include_critical):
        with _open_maybe_gz(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if args.include_critical:
                    key = (
                        entry.get("timestamp"),
                        entry.get("event_type"),
                        entry.get("message"),
                    )
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                counts[entry.get("event_type", "unknown")] += 1

    print(f"{'event_type':<35}  {'count':>8}")
    for et, n in counts.most_common():
        print(f"{et:<35}  {n:>8}")
    return dict(counts)


def cmd_maintain(args, log_dir: Path) -> Dict[str, int]:
    out = rotation_pass(
        log_dir,
        grace_days=args.grace_days,
        delete_after_days=args.delete_after_days,
    )
    print(
        f"gzipped: {out['gzipped']}, "
        f"deleted: {out['deleted']}, "
        f"skipped_critical: {out['skipped_critical']}"
    )
    return out


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Phase 5 structured log query CLI")
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_search = sub.add_parser("search")
    p_search.add_argument("--since", type=date.fromisoformat)
    p_search.add_argument("--until", type=date.fromisoformat)
    p_search.add_argument("--level")
    p_search.add_argument("--event-type")
    p_search.add_argument("--source")
    p_search.add_argument("--payload", action="append")
    p_search.add_argument("--include-critical", action="store_true")
    p_search.add_argument("--limit", type=int, default=100)
    p_search.add_argument("--format", choices=["json", "table"], default="json")
    p_search.set_defaults(func=cmd_search)

    p_tail = sub.add_parser("tail")
    p_tail.add_argument("--include-critical", action="store_true")
    p_tail.add_argument("--poll-ms", type=int, default=500)
    p_tail.set_defaults(func=cmd_tail)

    p_sum = sub.add_parser("summary")
    p_sum.add_argument("--since", type=date.fromisoformat)
    p_sum.add_argument("--until", type=date.fromisoformat)
    p_sum.add_argument("--include-critical", action="store_true")
    p_sum.set_defaults(func=cmd_summary)

    p_main = sub.add_parser("maintain")
    p_main.add_argument("--grace-days", type=int, default=7)
    p_main.add_argument("--delete-after-days", type=int, default=90)
    p_main.set_defaults(func=cmd_maintain)

    args = parser.parse_args(argv)
    args.func(args, args.log_dir)


if __name__ == "__main__":
    main()
