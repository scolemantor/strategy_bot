"""Pydantic request/response schemas for dashboard API."""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=1, max_length=128)


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    username: str
    email: str
    created_at: datetime
    last_login_at: Optional[datetime] = None


class TodayCandidate(BaseModel):
    ticker: str
    composite_score: float
    n_scanners: int
    n_categories: int
    directions: str
    scanners_hit: str
    categories_hit: str
    is_conflict: bool
    reasons: str


class TodayResponse(BaseModel):
    date: str
    total_count: int
    conflicts_count: int
    candidates: List[TodayCandidate]
    scanner_breakdown: List[dict]


class WatchlistMember(BaseModel):
    ticker: str
    status: str  # NEW | STRONGER | WEAKER | EXITED | STABLE
    composite_score: Optional[float] = None
    scanners_hit: Optional[str] = None
    delta_flag: Optional[str] = None
    stale_flag: Optional[str] = None
    scanner_reason: Optional[str] = None


class WatchlistResponse(BaseModel):
    date: str
    members: List[WatchlistMember]


# --- Phase 8a: extended watchlist + technical schemas ---

class WatchlistEntry(BaseModel):
    """Extended Phase 8a entry shape returned by GET /api/watchlist/entries."""
    ticker: str
    tier: int = 2
    position_size: Optional[int] = None
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    target_price: Optional[float] = None
    notes: str = ""
    auto_added: bool = False
    added_at: str = ""
    last_modified: str = ""
    # Legacy fields preserved for transition period
    added_date: str = ""
    reason: str = ""
    category: str = "general"
    # Latest technical scan, if available (None if no scan yet for this ticker)
    latest_technicals: Optional[dict] = None
    # Phase 8c Issue 2: transient hint set ONLY on the POST /entries
    # response — true if the post-add background scan was successfully
    # spawned. Frontend reads this to show a "Scanning..." spinner on
    # the just-added ticker for ~30s until the next poll picks up the
    # populated latest_technicals. Always None on GET responses.
    scan_triggered: Optional[bool] = None


class WatchlistEntriesResponse(BaseModel):
    entries: List[WatchlistEntry]
    last_technical_scan: Optional[str] = None


class WatchlistAddRequest(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=10)
    reason: str = ""
    source: str = "dashboard"  # "dashboard" | "cli" | "auto"
    tier: int = Field(2, ge=1, le=3)
    notes: str = ""
    category: str = "general"
    position_size: Optional[int] = Field(None, ge=0)
    entry_price: Optional[float] = Field(None, gt=0)
    stop_loss: Optional[float] = Field(None, gt=0)
    target_price: Optional[float] = Field(None, gt=0)


class WatchlistRemoveRequest(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=10)
    source: str = "dashboard"


class WatchlistUpdateRequest(BaseModel):
    """Partial update — only fields you want to change. Audit fields
    (added_at, last_modified) cannot be updated client-side."""
    tier: Optional[int] = Field(None, ge=1, le=3)
    position_size: Optional[int] = Field(None, ge=0)
    entry_price: Optional[float] = Field(None, gt=0)
    stop_loss: Optional[float] = Field(None, gt=0)
    target_price: Optional[float] = Field(None, gt=0)
    notes: Optional[str] = None
    reason: Optional[str] = None
    category: Optional[str] = None


class TechnicalDetail(BaseModel):
    """Full per-ticker technical breakdown — shape mirrors the JSON the
    technical_overlay scanner writes to data_cache/technical/<TICKER>.json."""
    ticker: str
    computed_at: str
    last_close: float
    setup_score: Optional[float] = None
    reason: Optional[str] = None
    trend: dict
    momentum: dict
    volume: dict
    volatility: dict
    key_levels: dict


class TickerMeta(BaseModel):
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    market_cap: Optional[float] = None
    last_updated: Optional[str] = None


class TickerScannerHit(BaseModel):
    date: str
    scanners: List[str]
    composite_score: Optional[float] = None


class TickerSignal(BaseModel):
    date: str
    scanner: str
    summary: str


class TickerResponse(BaseModel):
    meta: TickerMeta
    fundamentals: dict
    scanner_history: List[TickerScannerHit]
    recent_signals: List[TickerSignal]
    cached_at: Optional[str] = None


class HistoryEntry(BaseModel):
    date: str
    candidate_count: int
    scanner_count: int
    top_5: List[str]


class NotificationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    sent_at: datetime
    event_type: str
    title: Optional[str]
    message: str
    priority: int
    outcome: str
    suppression_reason: Optional[str]
    pushover_response_status: Optional[int]


class NotificationsResponse(BaseModel):
    total: int
    items: List[NotificationOut]


class ScannerSetting(BaseModel):
    name: str
    enabled: bool
    weight: float
    direction: str
    category: str
    last_updated: Optional[datetime] = None
    last_updated_by: Optional[str] = None


class SettingsResponse(BaseModel):
    scanners: List[ScannerSetting]


class ScannerSettingUpdate(BaseModel):
    enabled: Optional[bool] = None
    weight: Optional[float] = Field(None, ge=0.0, le=5.0)
