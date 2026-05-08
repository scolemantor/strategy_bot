import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api";
import { Column, DataTable } from "../components/DataTable";
import { LoadingSpinner } from "../components/LoadingSpinner";
import { ScoreBar } from "../components/ScoreBar";
import type { WatchlistMember, WatchlistResponse } from "../types";

const STATUS_FILTERS = ["ALL", "NEW", "STRONGER", "WEAKER", "EXITED", "STABLE"] as const;
type StatusFilter = (typeof STATUS_FILTERS)[number];

const STATUS_STYLE: Record<string, string> = {
  NEW: "bg-accent/20 text-accent border-accent/30",
  STRONGER: "bg-bull/20 text-bull border-bull/30",
  WEAKER: "bg-amber-500/20 text-amber-300 border-amber-500/30",
  EXITED: "bg-bear/20 text-bear border-bear/30",
  STABLE: "bg-slate-500/20 text-slate-400 border-slate-500/30",
};

export function Watchlist() {
  const navigate = useNavigate();
  const [data, setData] = useState<WatchlistResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<StatusFilter>("ALL");

  useEffect(() => {
    let cancelled = false;
    api
      .get<WatchlistResponse>("/api/watchlist")
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .catch(() => {
        if (!cancelled) setError("Failed to load watchlist.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const filtered = useMemo(() => {
    if (!data) return [];
    if (filter === "ALL") return data.members;
    return data.members.filter((m) => m.status === filter);
  }, [data, filter]);

  const columns: Column<WatchlistMember>[] = [
    {
      key: "ticker",
      header: "Ticker",
      accessor: (r) => r.ticker,
      sortable: true,
      render: (r) => <span className="font-semibold text-slate-100">{r.ticker}</span>,
    },
    {
      key: "status",
      header: "Status",
      accessor: (r) => r.status,
      sortable: true,
      render: (r) => (
        <span
          className={`inline-flex px-2 py-0.5 rounded text-[10px] font-semibold border ${
            STATUS_STYLE[r.status] ?? STATUS_STYLE.STABLE
          }`}
        >
          {r.status}
        </span>
      ),
    },
    {
      key: "score",
      header: "Score",
      accessor: (r) => r.composite_score ?? 0,
      sortable: true,
      render: (r) =>
        r.composite_score == null ? (
          <span className="text-slate-500">—</span>
        ) : (
          <ScoreBar score={r.composite_score} max={500} />
        ),
    },
    {
      key: "scanners",
      header: "Scanners",
      accessor: (r) => r.scanners_hit ?? "",
      render: (r) => (
        <span className="text-xs text-slate-400">{r.scanners_hit ?? "—"}</span>
      ),
    },
    {
      key: "reason",
      header: "Reason",
      accessor: (r) => r.scanner_reason ?? "",
      render: (r) => (
        <span className="text-xs text-slate-400 line-clamp-2 block max-w-md">
          {r.scanner_reason ?? "—"}
        </span>
      ),
    },
  ];

  return (
    <div>
      <div className="flex items-baseline justify-between mb-4">
        <h1 className="text-xl font-semibold">Watchlist</h1>
        {data && (
          <span className="text-sm text-slate-400">
            {data.members.length} members — {data.date}
          </span>
        )}
      </div>

      {loading && <LoadingSpinner />}
      {error && <div className="text-bear">{error}</div>}

      {data && (
        <>
          <div className="flex gap-1 mb-4">
            {STATUS_FILTERS.map((s) => (
              <button
                key={s}
                onClick={() => setFilter(s)}
                className={`px-3 py-1.5 rounded border text-xs uppercase tracking-wide ${
                  filter === s
                    ? "bg-accent text-ink border-accent"
                    : "bg-panel border-slate-700 text-slate-400 hover:text-slate-200"
                }`}
              >
                {s}
              </button>
            ))}
          </div>
          <DataTable
            rows={filtered}
            columns={columns}
            rowKey={(r) => r.ticker}
            onRowClick={(r) => navigate(`/ticker/${r.ticker}`)}
            emptyMessage="No watchlist members match this filter."
          />
        </>
      )}
    </div>
  );
}
