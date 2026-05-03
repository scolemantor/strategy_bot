"""Macro economic calendar scanner.

Surfaces upcoming macro events that move the broader market: FOMC meetings,
CPI/PPI/NFP/GDP releases, jobless claims, consumer confidence reports.

Use case: RISK AWARENESS. If FOMC is on Wednesday and you have a leveraged
position, you want to know. If CPI prints tomorrow morning, you want to know.
This scanner does NOT recommend trades — it surfaces calendar risk.

Source:
  - FOMC 2026/2027 dates: hardcoded from federalreserve.gov annual schedule
  - BLS releases (NFP, CPI, PPI): computed from known monthly patterns
    - NFP: 1st Friday of each month at 8:30 AM ET
    - CPI: typically Tuesday/Wednesday in 2nd week of month at 8:30 AM ET (varies)
    - PPI: day before CPI, also 8:30 AM ET
    - Retail Sales: mid-month, varies
  - Other regular releases: GDP advance estimate (last Thursday of Jan/Apr/Jul/Oct)

Honest limits:
  - Hardcoded FOMC dates need annual update (we'll update 2027 list before year end)
  - BLS dates computed approximately — actual release dates can shift by 1-2 days
    around holidays. Acceptable for risk-awareness use case.
  - Doesn't include international events (ECB, BOJ, China data) — US-only focus
  - Doesn't include earnings or company-specific events (those are scanners 5, 10)

This scanner has zero external API dependencies. Pure computation from
hardcoded schedules. Fastest, most reliable scanner in the suite.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List

import pandas as pd

from .base import Scanner, ScanResult, empty_result

log = logging.getLogger(__name__)

# Hardcoded FOMC meeting dates (day-2 of meeting = decision day)
# Source: federalreserve.gov/monetarypolicy/fomccalendars.htm
# Decision is announced 2:00 PM ET on day 2; press conference 2:30 PM ET
FOMC_MEETINGS_2026 = [
    ("2026-01-28", "FOMC Decision + Powell Press Conf", True),  # SEP / dot plot
    ("2026-03-18", "FOMC Decision + Powell Press Conf", True),  # SEP / dot plot
    ("2026-04-29", "FOMC Decision + Powell Press Conf", False),
    ("2026-06-17", "FOMC Decision + Powell Press Conf", True),  # SEP / dot plot
    ("2026-07-29", "FOMC Decision + Powell Press Conf", False),
    ("2026-09-16", "FOMC Decision + Powell Press Conf", True),  # SEP / dot plot
    ("2026-10-28", "FOMC Decision + Powell Press Conf", False),
    ("2026-12-09", "FOMC Decision + Powell Press Conf", True),  # SEP / dot plot
]
# 2027 dates - tentative until confirmed at preceding 2026 meeting; conservative estimates
FOMC_MEETINGS_2027 = [
    # Will update once Fed publishes 2027 calendar (typically late 2026)
]

# FOMC minutes are released exactly 3 weeks (21 days) after meeting

# BLS schedule patterns (computed at runtime)
# NFP: first Friday of month at 8:30 AM ET
# CPI: typically 10-13 of month
# PPI: typically day before or after CPI


def _first_friday_of_month(year: int, month: int) -> date:
    """Return the date of the first Friday of the given year/month."""
    d = date(year, month, 1)
    days_until_friday = (4 - d.weekday()) % 7  # Monday=0, Friday=4
    return d + timedelta(days=days_until_friday)


def _nth_weekday_of_month(year: int, month: int, weekday: int, nth: int) -> date:
    """Return the nth occurrence of `weekday` (0=Mon, 6=Sun) in the given month."""
    d = date(year, month, 1)
    days_until = (weekday - d.weekday()) % 7
    first = d + timedelta(days=days_until)
    return first + timedelta(weeks=nth - 1)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """Return the last occurrence of `weekday` in the given month."""
    # Start from end of month and walk back
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    days_back = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=days_back)


class MacroCalendarScanner(Scanner):
    name = "macro_calendar"
    description = "Upcoming FOMC, CPI, NFP, and other macro events in next 14 days (risk awareness)"
    cadence = "daily"  # Calendar events shift; want fresh checks daily

    LOOKAHEAD_DAYS = 14

    # Impact ratings — used for scoring + reason text
    IMPACT_HIGH = "HIGH"
    IMPACT_MEDIUM = "MEDIUM"
    IMPACT_LOW = "LOW"

    def run(self, run_date: date) -> ScanResult:
        log.info(f"Lookahead: {self.LOOKAHEAD_DAYS} days")
        window_end = run_date + timedelta(days=self.LOOKAHEAD_DAYS)
        log.info(f"Window: {run_date} to {window_end}")

        events: List[Dict] = []

        # 1. FOMC meetings
        for date_str, name, has_sep in FOMC_MEETINGS_2026 + FOMC_MEETINGS_2027:
            event_date = date.fromisoformat(date_str)
            if run_date <= event_date <= window_end:
                event_name = name
                if has_sep:
                    event_name += " (SEP/Dot Plot)"
                events.append({
                    "date": event_date,
                    "time_et": "14:00",
                    "name": event_name,
                    "agency": "Federal Reserve",
                    "impact": self.IMPACT_HIGH,
                })

        # 2. FOMC minutes (3 weeks after each meeting)
        for date_str, _, _ in FOMC_MEETINGS_2026 + FOMC_MEETINGS_2027:
            meeting_date = date.fromisoformat(date_str)
            minutes_date = meeting_date + timedelta(days=21)
            # Minutes typically released on Wednesdays around 2 PM
            # Adjust if weekend
            while minutes_date.weekday() >= 5:
                minutes_date += timedelta(days=1)
            if run_date <= minutes_date <= window_end:
                events.append({
                    "date": minutes_date,
                    "time_et": "14:00",
                    "name": f"FOMC Minutes (from {meeting_date})",
                    "agency": "Federal Reserve",
                    "impact": self.IMPACT_HIGH,
                })

        # 3. NFP (Non-Farm Payrolls) — 1st Friday of each month
        for offset_months in range(2):
            target_year = run_date.year
            target_month = run_date.month + offset_months
            if target_month > 12:
                target_year += 1
                target_month -= 12
            nfp = _first_friday_of_month(target_year, target_month)
            if run_date <= nfp <= window_end:
                events.append({
                    "date": nfp,
                    "time_et": "08:30",
                    "name": "Non-Farm Payrolls (NFP)",
                    "agency": "BLS",
                    "impact": self.IMPACT_HIGH,
                })

        # 4. CPI — usually 2nd week of month, Tuesday/Wednesday around the 10th-13th
        # We approximate as the 2nd Wednesday of the month
        for offset_months in range(2):
            target_year = run_date.year
            target_month = run_date.month + offset_months
            if target_month > 12:
                target_year += 1
                target_month -= 12
            cpi = _nth_weekday_of_month(target_year, target_month, 2, 2)  # 2nd Wednesday
            if run_date <= cpi <= window_end:
                events.append({
                    "date": cpi,
                    "time_et": "08:30",
                    "name": "Consumer Price Index (CPI)",
                    "agency": "BLS",
                    "impact": self.IMPACT_HIGH,
                })

        # 5. PPI — typically day before CPI, also 8:30 AM ET
        for offset_months in range(2):
            target_year = run_date.year
            target_month = run_date.month + offset_months
            if target_month > 12:
                target_year += 1
                target_month -= 12
            cpi_date = _nth_weekday_of_month(target_year, target_month, 2, 2)
            ppi = cpi_date - timedelta(days=1)
            if run_date <= ppi <= window_end:
                events.append({
                    "date": ppi,
                    "time_et": "08:30",
                    "name": "Producer Price Index (PPI)",
                    "agency": "BLS",
                    "impact": self.IMPACT_MEDIUM,
                })

        # 6. Retail Sales — typically mid-month around 14th-16th
        for offset_months in range(2):
            target_year = run_date.year
            target_month = run_date.month + offset_months
            if target_month > 12:
                target_year += 1
                target_month -= 12
            rs = _nth_weekday_of_month(target_year, target_month, 1, 3)  # 3rd Tuesday
            if run_date <= rs <= window_end:
                events.append({
                    "date": rs,
                    "time_et": "08:30",
                    "name": "Retail Sales",
                    "agency": "Census Bureau",
                    "impact": self.IMPACT_MEDIUM,
                })

        # 7. GDP advance estimate — last Thursday of Jan/Apr/Jul/Oct
        for offset_months in range(3):
            target_year = run_date.year
            target_month = run_date.month + offset_months
            if target_month > 12:
                target_year += 1
                target_month -= 12
            if target_month not in (1, 4, 7, 10):
                continue
            gdp = _last_weekday_of_month(target_year, target_month, 3)  # last Thursday
            if run_date <= gdp <= window_end:
                events.append({
                    "date": gdp,
                    "time_et": "08:30",
                    "name": "GDP Advance Estimate",
                    "agency": "BEA",
                    "impact": self.IMPACT_HIGH,
                })

        # 8. ISM Manufacturing PMI — 1st business day of each month
        for offset_months in range(2):
            target_year = run_date.year
            target_month = run_date.month + offset_months
            if target_month > 12:
                target_year += 1
                target_month -= 12
            # 1st business day = 1st of month, skip weekend
            ism = date(target_year, target_month, 1)
            while ism.weekday() >= 5:
                ism += timedelta(days=1)
            if run_date <= ism <= window_end:
                events.append({
                    "date": ism,
                    "time_et": "10:00",
                    "name": "ISM Manufacturing PMI",
                    "agency": "ISM",
                    "impact": self.IMPACT_MEDIUM,
                })

        # 9. Initial Jobless Claims — every Thursday at 8:30 AM ET
        cursor = run_date
        while cursor <= window_end:
            if cursor.weekday() == 3:  # Thursday
                events.append({
                    "date": cursor,
                    "time_et": "08:30",
                    "name": "Initial Jobless Claims",
                    "agency": "DOL",
                    "impact": self.IMPACT_LOW,
                })
            cursor += timedelta(days=1)

        # 10. Consumer Confidence Index (Conference Board) — last Tuesday of each month
        for offset_months in range(2):
            target_year = run_date.year
            target_month = run_date.month + offset_months
            if target_month > 12:
                target_year += 1
                target_month -= 12
            cci = _last_weekday_of_month(target_year, target_month, 1)  # last Tuesday
            if run_date <= cci <= window_end:
                events.append({
                    "date": cci,
                    "time_et": "10:00",
                    "name": "Consumer Confidence Index",
                    "agency": "Conference Board",
                    "impact": self.IMPACT_MEDIUM,
                })

        if not events:
            log.info("No macro events in window")
            return empty_result(self.name, run_date)

        # Build output rows
        impact_score = {self.IMPACT_HIGH: 30, self.IMPACT_MEDIUM: 15, self.IMPACT_LOW: 5}

        rows = []
        for e in events:
            days_until = (e["date"] - run_date).days
            urgency_bonus = max(0, 10 - days_until)  # imminent events score higher
            score = impact_score[e["impact"]] + urgency_bonus
            rows.append({
                "event_date": e["date"].isoformat(),
                "event_time_et": e["time_et"],
                "event_name": e["name"],
                "agency": e["agency"],
                "impact": e["impact"],
                "days_until": days_until,
                "score": score,
                "reason": (
                    f"{e['name']} on {e['date']} at {e['time_et']} ET "
                    f"({e['impact']} impact, in {days_until} day{'s' if days_until != 1 else ''})"
                ),
            })

        df = pd.DataFrame(rows).sort_values(["event_date", "score"], ascending=[True, False]).reset_index(drop=True)

        log.info(f"Found {len(rows)} macro events in window")
        for _, r in df.iterrows():
            log.info(f"  {r['event_date']} {r['event_time_et']} ET — {r['event_name']:<45} [{r['impact']}]")

        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=df,
            notes=[
                f"Window: {run_date} to {window_end} ({self.LOOKAHEAD_DAYS} days)",
                f"Total events: {len(rows)}",
                f"HIGH impact: {sum(1 for r in rows if r['impact'] == self.IMPACT_HIGH)}",
                f"MEDIUM impact: {sum(1 for r in rows if r['impact'] == self.IMPACT_MEDIUM)}",
                f"LOW impact: {sum(1 for r in rows if r['impact'] == self.IMPACT_LOW)}",
                "Risk-awareness scanner: not a trade signal. Use to manage existing positions ahead of events.",
                "FOMC dates hardcoded from federalreserve.gov; needs annual update for 2027.",
            ],
        )