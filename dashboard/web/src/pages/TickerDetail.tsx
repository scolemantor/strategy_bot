import { useCallback, useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { api, ApiError } from "../api";
import { LoadingSpinner } from "../components/LoadingSpinner";
import { ScannerBadge } from "../components/ScannerBadge";
import { ScoreBar } from "../components/ScoreBar";
import type { TickerResponse } from "../types";

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

export function TickerDetail() {
  const { symbol } = useParams<{ symbol: string }>();
  const [data, setData] = useState<TickerResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

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
      } catch (e) {
        // Phase 8c Issue C diagnostic — surface the actual status so we can
        // tell 401/404/500 apart instead of all paths showing the same
        // generic "Failed to load" string.
        const detail =
          e instanceof ApiError
            ? `API ${e.status}${e.body ? `: ${JSON.stringify(e.body).slice(0, 200)}` : ""}`
            : String(e);
        console.error(`[TickerDetail] load(${upper}, refresh=${refresh}) failed:`, e);
        setError(`Failed to load ticker (${detail})`);
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
  }, [load]);

  if (loading) return <LoadingSpinner label="Loading ticker..." />;
  if (error) return <div className="text-bear">{error}</div>;
  if (!data) return null;

  const { meta, fundamentals, scanner_history, recent_signals } = data;

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

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-panel border border-slate-800 rounded p-3">
      <div className="text-[10px] uppercase tracking-wide text-slate-500">{label}</div>
      <div className="text-slate-100 font-mono mt-1">{value}</div>
    </div>
  );
}
