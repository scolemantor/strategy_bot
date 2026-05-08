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
