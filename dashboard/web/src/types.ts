// Shared TypeScript interfaces. These mirror dashboard/api/schemas.py —
// keep them in sync when API shapes change.

export interface User {
  id: number;
  username: string;
  email: string;
  created_at: string;
  last_login_at: string | null;
}

export interface TodayCandidate {
  ticker: string;
  composite_score: number;
  n_scanners: number;
  n_categories: number;
  directions: string;
  scanners_hit: string;
  categories_hit: string;
  is_conflict: boolean;
  reasons: string;
}

export interface ScannerBreakdown {
  scanner: string;
  direction: string;
  category: string;
  weight: number;
  candidates: number;
  contributed_to_master: number;
}

export interface TodayResponse {
  date: string;
  total_count: number;
  conflicts_count: number;
  candidates: TodayCandidate[];
  scanner_breakdown: ScannerBreakdown[];
}

export interface WatchlistMember {
  ticker: string;
  status: "NEW" | "STRONGER" | "WEAKER" | "EXITED" | "STABLE";
  composite_score: number | null;
  scanners_hit: string | null;
  delta_flag: string | null;
  stale_flag: string | null;
  scanner_reason: string | null;
}

export interface WatchlistResponse {
  date: string;
  members: WatchlistMember[];
}

export interface TickerScannerHit {
  date: string;
  scanners: string[];
  composite_score: number | null;
}

export interface TickerSignal {
  date: string;
  scanner: string;
  summary: string;
}

export interface TickerMeta {
  symbol: string;
  name: string | null;
  sector: string | null;
  industry: string | null;
  market_cap: number | null;
  last_updated: string | null;
}

export interface TickerResponse {
  meta: TickerMeta;
  fundamentals: Record<string, unknown>;
  scanner_history: TickerScannerHit[];
  recent_signals: TickerSignal[];
  cached_at: string | null;
  // Phase 8c polish: full technical breakdown (null if no scan yet)
  technical_breakdown: TechnicalDetail | null;
}

export interface TickerRescanResponse {
  ticker: string;
  scan_triggered: boolean;
}

export interface HistoryEntry {
  date: string;
  candidate_count: number;
  scanner_count: number;
  top_5: string[];
}

export interface NotificationItem {
  id: number;
  sent_at: string;
  event_type: string;
  title: string | null;
  message: string;
  priority: number;
  outcome: "dispatched" | "failed" | "suppressed" | "test_mode";
  suppression_reason: string | null;
  pushover_response_status: number | null;
}

export interface NotificationsResponse {
  total: number;
  items: NotificationItem[];
}

export interface ScannerSetting {
  name: string;
  enabled: boolean;
  weight: number;
  direction: string;
  category: string;
  last_updated: string | null;
  last_updated_by: string | null;
}

export interface SettingsResponse {
  scanners: ScannerSetting[];
}


// --- Phase 8a watchlist + technical types (mirror dashboard/api/schemas.py) ---

export interface TechnicalTrend {
  ma_20: number | null;
  ma_50: number | null;
  ma_200: number | null;
  above_ma_20: boolean | null;
  above_ma_50: boolean | null;
  above_ma_200: boolean | null;
  ma_20_slope: string;
  ma_50_slope: string;
  ma_200_slope: string;
  golden_cross_recent: boolean;
  death_cross_recent: boolean;
}

export interface TechnicalMomentum {
  rsi_14: number | null;
  rsi_class: string | null;
  macd_hist: number | null;
  macd_above_signal: boolean | null;
  macd_recent_cross: string | null;
  roc_5d: number | null;
  roc_10d: number | null;
  roc_20d: number | null;
}

export interface TechnicalVolume {
  last_volume: number | null;
  vol_ma_20: number | null;
  vol_ratio_20d: number | null;
  obv_trend_30d: string | null;
  up_down_vol_ratio_30d: number | null;
}

export interface TechnicalVolatility {
  atr_14: number | null;
  bb_lower: number | null;
  bb_upper: number | null;
  bb_position: number | null;
  hv_30d_annualized: number | null;
}

export interface TechnicalKeyLevels {
  high_30d: number | null;
  low_30d: number | null;
  high_52w: number | null;
  low_52w: number | null;
  pct_from_52w_high: number | null;
  pct_from_52w_low: number | null;
}

export interface TechnicalDetail {
  ticker: string;
  computed_at: string;
  last_close: number;
  setup_score: number | null;
  reason: string | null;
  // Phase 8c Issue 3 narration. null when SDK missing, ANTHROPIC_API_KEY
  // unset, data_sufficiency != "full", or API call failed.
  narrative: string | null;
  data_sufficiency: "full" | "partial" | "minimal";
  bar_count: number;
  trend: TechnicalTrend;
  momentum: TechnicalMomentum;
  volume: TechnicalVolume;
  volatility: TechnicalVolatility;
  key_levels: TechnicalKeyLevels;
}

export interface WatchlistEntry {
  ticker: string;
  tier: 1 | 2 | 3;
  position_size: number | null;
  entry_price: number | null;
  stop_loss: number | null;
  target_price: number | null;
  notes: string;
  reason: string;
  category: string;
  auto_added: boolean;
  added_at: string;
  last_modified: string;
  added_date: string;
  latest_technicals: TechnicalDetail | null;
  // Phase 8c Issue 2: transient hint, set ONLY on POST /entries
  // response. true if post-add background scan spawned cleanly. Always
  // null on GET. Frontend uses this to show a "Scanning..." spinner on
  // the new ticker for ~30s until polling picks up populated
  // latest_technicals.
  scan_triggered?: boolean | null;
}

export interface WatchlistEntriesResponse {
  entries: WatchlistEntry[];
  last_technical_scan: string | null;
}

export interface WatchlistAddRequest {
  ticker: string;
  reason?: string;
  source?: "dashboard" | "cli" | "auto";
  tier?: 1 | 2 | 3;
  notes?: string;
  category?: string;
  position_size?: number;
  entry_price?: number;
  stop_loss?: number;
  target_price?: number;
}
