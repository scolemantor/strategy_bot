import { useCallback, useEffect, useRef, useState } from "react";
import { useParams } from "react-router-dom";
import { api, ApiError } from "../api";
import { LoadingSpinner } from "../components/LoadingSpinner";
import { ScannerBadge } from "../components/ScannerBadge";
import { ScoreBar } from "../components/ScoreBar";
import { toast } from "../components/Toast";
import type {
  TechnicalDetail,
  TickerRescanResponse,
  TickerResponse,
} from "../types";

// Phase 8c polish: ticker detail now surfaces the per-ticker technical
// breakdown written by scanners/technical_overlay.py. Two new sections
// (Technical Setup + Indicators) inserted between the existing Overview
// and Scanner History blocks. A "Rescan" button next to "Refresh from
// yfinance" fires POST /api/ticker/{sym}/rescan and polls every 5s for
// up to 60s for a fresher computed_at to land on disk.

const RESCAN_POLL_INTERVAL_MS = 5_000;
const RESCAN_POLL_MAX_MS = 60_000;

function formatNumber(v: unknown): string {
  if (v == null || v === "") return "—";
  if (typeof v === "number") {
    if (Math.abs(v) >= 1e9) return `${(v / 1e9).toFixed(2)}B`;
    if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
    if (Math.abs(v) >= 1e3) return `${(v / 1e3).toFixed(2)}K`;
    return v.toFixed(2);
  }
  return String(v);
}

function formatCurrency(v: unknown): string {
  if (v == null || v === "") return "—";
  if (typeof v === "number") return `$${formatNumber(v)}`;
  return String(v);
}

function formatPct(v: unknown): string {
  if (v == null || typeof v !== "number") return "—";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(2)}%`;
}

function formatX(v: unknown): string {
  if (v == null || typeof v !== "number") return "—";
  return `${v.toFixed(2)}x`;
}

function formatBool(v: boolean | null | undefined, yes = "Yes", no = "No"): string {
  if (v === null || v === undefined) return "—";
  return v ? yes : no;
}

function relativeTime(iso: string | null | undefined): string {
  if (!iso) return "never";
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return iso;
  const diffSec = Math.round((Date.now() - then) / 1000);
  if (diffSec < 0) return "just now";
  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffSec < 3_600) return `${Math.round(diffSec / 60)}m ago`;
  if (diffSec < 86_400) return `${Math.round(diffSec / 3_600)}h ago`;
  return `${Math.round(diffSec / 86_400)}d ago`;
}

function scoreColorClass(score: number | null | undefined): string {
  if (score == null) return "text-slate-400";
  if (score < 30) return "text-bear";
  if (score < 60) return "text-conflict";
  return "text-bull";
}

function scoreBgClass(score: number | null | undefined): string {
  if (score == null) return "bg-slate-700/30 border-slate-600";
  if (score < 30) return "bg-bear/10 border-bear/30";
  if (score < 60) return "bg-conflict/10 border-conflict/30";
  return "bg-bull/10 border-bull/30";
}

function narrativeFallback(td: TechnicalDetail): string {
  // Backend doesn't write narrative_skip_reason today; future-proof by
  // checking it first if the scanner ever starts emitting it.
  const reason = (td as unknown as { narrative_skip_reason?: string })
    .narrative_skip_reason;
  if (reason) return reason;
  if (td.data_sufficiency !== "full") {
    return "Narrative unavailable — needs 200+ bars of history (recently IPO'd or new ticker)";
  }
  return "Narrative unavailable for this ticker";
}

export function TickerDetail() {
  const { symbol } = useParams<{ symbol: string }>();
  const [data, setData] = useState<TickerResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [scanning, setScanning] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Snapshot of computed_at at rescan-click time. Polling stops as soon
  // as a fetched response has a different (newer) value.
  const scanStartComputedAt = useRef<string | null>(null);
  const scanTimeoutId = useRef<ReturnType<typeof setTimeout> | null>(null);

  const load = useCallback(
    async (refresh = false) => {
      if (!symbol) return;
      const upper = symbol.toUpperCase();
      try {
        const resp = refresh
          ? await api.post<TickerResponse>(`/api/ticker/${upper}/refresh`)
          : await api.get<TickerResponse>(`/api/ticker/${upper}`);
        setData(resp);
        setError(null);
        return resp;
      } catch (e) {
        const detail =
          e instanceof ApiError
            ? `API ${e.status}${e.body ? `: ${JSON.stringify(e.body).slice(0, 200)}` : ""}`
            : String(e);
        console.error(`[TickerDetail] load(${upper}, refresh=${refresh}) failed:`, e);
        setError(`Failed to load ticker (${detail})`);
        return null;
      } finally {
        setLoading(false);
        setRefreshing(false);
      }
    },
    [symbol],
  );

  useEffect(() => {
    setLoading(true);
    load(false);
    // Cleanup any polling timers when the symbol changes or unmount.
    return () => {
      if (scanTimeoutId.current !== null) {
        clearTimeout(scanTimeoutId.current);
        scanTimeoutId.current = null;
      }
    };
  }, [load]);

  const handleRescan = useCallback(async () => {
    if (!symbol || scanning) return;
    const upper = symbol.toUpperCase();
    scanStartComputedAt.current = data?.technical_breakdown?.computed_at ?? null;

    setScanning(true);
    let resp: TickerRescanResponse;
    try {
      resp = await api.post<TickerRescanResponse>(`/api/ticker/${upper}/rescan`);
    } catch (e) {
      setScanning(false);
      const detail = e instanceof ApiError ? `API ${e.status}` : String(e);
      toast.error(`Failed to start rescan: ${detail}`);
      return;
    }
    if (!resp.scan_triggered) {
      setScanning(false);
      toast.error("Rescan failed to spawn — check server logs");
      return;
    }

    const startedAt = Date.now();
    const poll = async () => {
      // Silently retry network errors during polling — only the timeout
      // path produces a user-visible message.
      let next: TickerResponse | null = null;
      try {
        next = await api.get<TickerResponse>(`/api/ticker/${upper}`);
      } catch {
        next = null;
      }
      if (next) {
        setData(next);
        const newComputedAt = next.technical_breakdown?.computed_at ?? null;
        if (
          newComputedAt &&
          newComputedAt !== scanStartComputedAt.current
        ) {
          setScanning(false);
          toast.success(`Scan complete for ${upper}`);
          return;
        }
      }
      if (Date.now() - startedAt >= RESCAN_POLL_MAX_MS) {
        setScanning(false);
        toast.info(
          "Scan taking longer than expected — try refreshing the page in a moment",
        );
        return;
      }
      scanTimeoutId.current = setTimeout(poll, RESCAN_POLL_INTERVAL_MS);
    };
    scanTimeoutId.current = setTimeout(poll, RESCAN_POLL_INTERVAL_MS);
  }, [symbol, scanning, data]);

  if (loading) return <LoadingSpinner label="Loading ticker..." />;
  if (error) return <div className="text-bear">{error}</div>;
  if (!data) return null;

  const { meta, fundamentals, scanner_history, recent_signals, technical_breakdown } = data;

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold">{meta.symbol}</h1>
          {meta.name && <p className="text-slate-300">{meta.name}</p>}
          <p className="text-xs text-slate-500 mt-1">
            {meta.sector ?? "—"} · {meta.industry ?? "—"} · last updated{" "}
            {meta.last_updated ?? "never"}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={handleRescan}
            disabled={scanning}
            className="bg-panel2 border border-slate-700 text-slate-200 px-4 py-1.5 rounded font-semibold hover:bg-panel2/70 disabled:opacity-50"
            title="Run technical_overlay scanner for this ticker"
          >
            {scanning ? "Scanning..." : "Rescan"}
          </button>
          <button
            onClick={() => {
              setRefreshing(true);
              load(true);
            }}
            disabled={refreshing}
            className="bg-accent text-ink px-4 py-1.5 rounded font-semibold hover:bg-accent/90 disabled:opacity-50"
          >
            {refreshing ? "Refreshing..." : "Refresh from yfinance"}
          </button>
        </div>
      </div>

      <section>
        <h2 className="text-sm uppercase tracking-wide text-slate-400 mb-2">Overview</h2>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <Stat label="Market Cap" value={formatCurrency(meta.market_cap)} />
          <Stat label="Last Close" value={formatCurrency(fundamentals["last_close"])} />
          <Stat label="P/E (trailing)" value={formatNumber(fundamentals["pe_trailing"])} />
          <Stat label="P/B" value={formatNumber(fundamentals["pb"])} />
          <Stat label="EV/EBITDA" value={formatNumber(fundamentals["ev_ebitda"])} />
          <Stat label="Debt/Equity" value={formatNumber(fundamentals["debt_equity"])} />
          <Stat label="Free Cash Flow" value={formatCurrency(fundamentals["fcf"])} />
        </div>
      </section>

      <section>
        <h2 className="text-sm uppercase tracking-wide text-slate-400 mb-2">
          Technical Setup
        </h2>
        {technical_breakdown ? (
          <TechnicalSetup td={technical_breakdown} />
        ) : (
          <div className="bg-panel border border-slate-800 rounded-lg p-4 text-sm text-slate-400">
            <p>No technical scan yet for this ticker.</p>
            <button
              onClick={handleRescan}
              disabled={scanning}
              className="mt-3 bg-accent text-ink px-4 py-1.5 rounded font-semibold hover:bg-accent/90 disabled:opacity-50"
            >
              {scanning ? "Scanning..." : "Run scanner now"}
            </button>
          </div>
        )}
      </section>

      {technical_breakdown && <Indicators td={technical_breakdown} />}

      <section>
        <h2 className="text-sm uppercase tracking-wide text-slate-400 mb-2">
          Scanner History (30d)
        </h2>
        {scanner_history.length === 0 ? (
          <p className="text-slate-500 text-sm">No scanner appearances in the last 30 days.</p>
        ) : (
          <div className="border border-slate-800 rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-panel2 text-slate-300 uppercase text-[11px]">
                <tr>
                  <th className="px-3 py-2 text-left w-32">Date</th>
                  <th className="px-3 py-2 text-left w-32">Composite</th>
                  <th className="px-3 py-2 text-left">Scanners</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {scanner_history.map((h) => (
                  <tr key={h.date} className="bg-panel">
                    <td className="px-3 py-2 text-slate-400 tabular-nums">{h.date}</td>
                    <td className="px-3 py-2">
                      {h.composite_score != null ? (
                        <ScoreBar score={h.composite_score} max={5} />
                      ) : (
                        <span className="text-slate-500">—</span>
                      )}
                    </td>
                    <td className="px-3 py-2">
                      <div className="flex flex-wrap gap-1">
                        {h.scanners.map((s) => (
                          <ScannerBadge key={s} name={s} />
                        ))}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section>
        <h2 className="text-sm uppercase tracking-wide text-slate-400 mb-2">
          Recent Signals (7d)
        </h2>
        {recent_signals.length === 0 ? (
          <p className="text-slate-500 text-sm">No signals from the last 7 days.</p>
        ) : (
          <ul className="space-y-1.5">
            {recent_signals.map((s, i) => (
              <li
                key={`${s.date}-${s.scanner}-${i}`}
                className="bg-panel border border-slate-800 rounded p-3 text-sm"
              >
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-slate-400 tabular-nums text-xs">{s.date}</span>
                  <ScannerBadge name={s.scanner} />
                </div>
                <p className="text-slate-300 text-xs">{s.summary}</p>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}

function TechnicalSetup({ td }: { td: TechnicalDetail }) {
  const score = td.setup_score;
  return (
    <div className={`border rounded-lg p-4 ${scoreBgClass(score)}`}>
      <div className="flex items-baseline gap-4 flex-wrap">
        <div>
          <div className="text-[10px] uppercase tracking-wide text-slate-500">
            Setup Score
          </div>
          <div className={`text-3xl font-bold tabular-nums ${scoreColorClass(score)}`}>
            {score != null ? score.toFixed(0) : "—"}
            <span className="text-base text-slate-500 font-normal">/100</span>
          </div>
        </div>
        <div className="flex-1 min-w-0">
          {td.reason && (
            <div className="font-mono text-xs text-slate-300 break-words">
              {td.reason}
            </div>
          )}
          <div className="text-[11px] text-slate-500 mt-1">
            Updated {relativeTime(td.computed_at)} · {td.bar_count} bars ·{" "}
            <span
              className={
                td.data_sufficiency === "full"
                  ? "text-slate-400"
                  : "text-conflict"
              }
            >
              {td.data_sufficiency} data
            </span>
          </div>
        </div>
      </div>

      <div className="mt-4">
        {td.narrative ? (
          <div className="prose prose-invert prose-sm max-w-none text-slate-200 whitespace-pre-wrap">
            {td.narrative}
          </div>
        ) : (
          <p className="text-xs italic text-slate-500">{narrativeFallback(td)}</p>
        )}
      </div>
    </div>
  );
}

function Indicators({ td }: { td: TechnicalDetail }) {
  const tr = td.trend;
  const mo = td.momentum;
  const vo = td.volume;
  const vol = td.volatility;
  const kl = td.key_levels;

  return (
    <section className="space-y-5">
      <h2 className="text-sm uppercase tracking-wide text-slate-400 mb-2">
        Indicators
      </h2>

      <Subsection title="Trend">
        <Stat
          label="MA 20"
          value={formatCurrency(tr.ma_20)}
          sub={`${formatBool(tr.above_ma_20, "above", "below")} · ${tr.ma_20_slope}`}
        />
        <Stat
          label="MA 50"
          value={formatCurrency(tr.ma_50)}
          sub={`${formatBool(tr.above_ma_50, "above", "below")} · ${tr.ma_50_slope}`}
        />
        <Stat
          label="MA 200"
          value={formatCurrency(tr.ma_200)}
          sub={`${formatBool(tr.above_ma_200, "above", "below")} · ${tr.ma_200_slope}`}
        />
        <Stat
          label="MA Cross (30d)"
          value={
            tr.golden_cross_recent
              ? "Golden ↑"
              : tr.death_cross_recent
                ? "Death ↓"
                : "None"
          }
          valueClass={
            tr.golden_cross_recent
              ? "text-bull"
              : tr.death_cross_recent
                ? "text-bear"
                : "text-slate-400"
          }
        />
      </Subsection>

      <Subsection title="Momentum">
        <Stat
          label="RSI 14"
          value={mo.rsi_14 != null ? mo.rsi_14.toFixed(1) : "—"}
          sub={mo.rsi_class ?? undefined}
          valueClass={
            mo.rsi_14 == null
              ? "text-slate-400"
              : mo.rsi_14 >= 70
                ? "text-bear"
                : mo.rsi_14 <= 30
                  ? "text-bull"
                  : "text-slate-100"
          }
        />
        <Stat
          label="MACD Hist"
          value={mo.macd_hist != null ? mo.macd_hist.toFixed(3) : "—"}
          sub={`${formatBool(mo.macd_above_signal, "above signal", "below signal")}${
            mo.macd_recent_cross ? ` · ${mo.macd_recent_cross}` : ""
          }`}
          valueClass={
            mo.macd_above_signal == null
              ? "text-slate-400"
              : mo.macd_above_signal
                ? "text-bull"
                : "text-bear"
          }
        />
        <Stat label="ROC 5d" value={formatPct(mo.roc_5d)} />
        <Stat label="ROC 20d" value={formatPct(mo.roc_20d)} />
      </Subsection>

      <Subsection title="Volume + Volatility">
        <Stat
          label="Vol vs 20d avg"
          value={formatX(vo.vol_ratio_20d)}
          valueClass={
            vo.vol_ratio_20d == null
              ? "text-slate-400"
              : vo.vol_ratio_20d >= 1.5
                ? "text-bull"
                : vo.vol_ratio_20d < 0.7
                  ? "text-bear"
                  : "text-slate-100"
          }
        />
        <Stat label="OBV trend (30d)" value={vo.obv_trend_30d ?? "—"} />
        <Stat label="ATR 14" value={formatCurrency(vol.atr_14)} />
        <Stat
          label="BB Position"
          value={
            vol.bb_position != null ? `${(vol.bb_position * 100).toFixed(0)}%` : "—"
          }
          sub={
            vol.bb_position == null
              ? undefined
              : vol.bb_position >= 0.95
                ? "upper band"
                : vol.bb_position <= 0.05
                  ? "lower band"
                  : "mid"
          }
        />
        <Stat
          label="HV 30d (annualized)"
          value={vol.hv_30d_annualized != null ? formatPct(vol.hv_30d_annualized * 100) : "—"}
        />
        <Stat
          label="Up/Down vol (30d)"
          value={formatX(vo.up_down_vol_ratio_30d)}
          valueClass={
            vo.up_down_vol_ratio_30d == null
              ? "text-slate-400"
              : vo.up_down_vol_ratio_30d >= 1.2
                ? "text-bull"
                : vo.up_down_vol_ratio_30d <= 0.8
                  ? "text-bear"
                  : "text-slate-100"
          }
        />
      </Subsection>

      <Subsection title="Key Levels">
        <Stat label="52w High" value={formatCurrency(kl.high_52w)} />
        <Stat label="52w Low" value={formatCurrency(kl.low_52w)} />
        <Stat
          label="% from 52w High"
          value={formatPct(kl.pct_from_52w_high)}
          valueClass={
            kl.pct_from_52w_high == null
              ? "text-slate-400"
              : kl.pct_from_52w_high >= -3
                ? "text-bull"
                : kl.pct_from_52w_high <= -20
                  ? "text-bear"
                  : "text-slate-100"
          }
        />
        <Stat label="% from 52w Low" value={formatPct(kl.pct_from_52w_low)} />
        <Stat label="30d High" value={formatCurrency(kl.high_30d)} />
        <Stat label="30d Low" value={formatCurrency(kl.low_30d)} />
      </Subsection>
    </section>
  );
}

function Subsection({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <h3 className="text-xs uppercase tracking-wide text-slate-500 mb-2">{title}</h3>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">{children}</div>
    </div>
  );
}

function Stat({
  label,
  value,
  sub,
  valueClass,
}: {
  label: string;
  value: string;
  sub?: string;
  valueClass?: string;
}) {
  return (
    <div className="bg-panel border border-slate-800 rounded p-3">
      <div className="text-[10px] uppercase tracking-wide text-slate-500">{label}</div>
      <div className={`font-mono mt-1 ${valueClass ?? "text-slate-100"}`}>{value}</div>
      {sub && <div className="text-[10px] text-slate-500 mt-0.5">{sub}</div>}
    </div>
  );
}
