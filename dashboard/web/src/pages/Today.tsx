import { useEffect, useMemo, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { api, ApiError } from "../api";
import { Column, DataTable } from "../components/DataTable";
import { DirectionBadge } from "../components/DirectionBadge";
import { LoadingSpinner } from "../components/LoadingSpinner";
import { ScannerBadge } from "../components/ScannerBadge";
import { ScoreBar } from "../components/ScoreBar";
import type { TodayCandidate, TodayResponse } from "../types";

type DirectionFilter = "all" | "bull" | "bear" | "conflict";

function classifyDirection(c: TodayCandidate): DirectionFilter {
  if (c.is_conflict) return "conflict";
  if (c.directions.includes("bullish") && !c.directions.includes("bearish")) return "bull";
  if (c.directions.includes("bearish") && !c.directions.includes("bullish")) return "bear";
  return "all";
}

export function Today() {
  // /history/:date renders this same component with a target date URL param.
  const { date: routeDate } = useParams<{ date?: string }>();
  const navigate = useNavigate();
  const [data, setData] = useState<TodayResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [scannerFilter, setScannerFilter] = useState<string>("all");
  const [directionFilter, setDirectionFilter] = useState<DirectionFilter>("all");
  const [search, setSearch] = useState("");

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    const path = routeDate ? `/api/history/${routeDate}` : "/api/today";
    api
      .get<TodayResponse>(path)
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .catch((err) => {
        if (cancelled) return;
        if (err instanceof ApiError && err.status === 404) {
          setError("No scan output available for this date.");
        } else {
          setError("Failed to load.");
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [routeDate]);

  const filtered = useMemo(() => {
    if (!data) return [];
    return data.candidates.filter((c) => {
      if (scannerFilter !== "all" && !c.scanners_hit.includes(scannerFilter)) return false;
      if (directionFilter !== "all" && classifyDirection(c) !== directionFilter) return false;
      if (search && !c.ticker.toUpperCase().includes(search.toUpperCase())) return false;
      return true;
    });
  }, [data, scannerFilter, directionFilter, search]);

  const allScanners = useMemo(() => {
    if (!data) return [] as string[];
    const set = new Set<string>();
    for (const c of data.candidates) {
      for (const s of c.scanners_hit.split(",").map((x) => x.trim())) {
        if (s) set.add(s);
      }
    }
    return Array.from(set).sort();
  }, [data]);

  const columns: Column<TodayCandidate>[] = [
    {
      key: "rank",
      header: "#",
      accessor: () => 0,
      render: (_) => null,
      className: "w-10 text-slate-500",
    },
    {
      key: "ticker",
      header: "Ticker",
      accessor: (r) => r.ticker,
      sortable: true,
      render: (r) => <span className="font-semibold text-slate-100">{r.ticker}</span>,
    },
    {
      key: "score",
      header: "Composite",
      accessor: (r) => r.composite_score,
      sortable: true,
      render: (r) => <ScoreBar score={r.composite_score} max={5} />,
    },
    {
      key: "direction",
      header: "Direction",
      accessor: (r) => r.directions,
      sortable: true,
      render: (r) => <DirectionBadge directions={r.directions} isConflict={r.is_conflict} />,
    },
    {
      key: "n",
      header: "N",
      accessor: (r) => r.n_scanners,
      sortable: true,
      className: "text-slate-400 tabular-nums w-12",
    },
    {
      key: "scanners",
      header: "Scanners",
      accessor: (r) => r.scanners_hit,
      render: (r) => (
        <div className="flex flex-wrap gap-1">
          {r.scanners_hit
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean)
            .map((s) => (
              <ScannerBadge key={s} name={s} />
            ))}
        </div>
      ),
    },
    {
      key: "reasons",
      header: "Summary",
      accessor: (r) => r.reasons,
      render: (r) => (
        <span className="text-xs text-slate-400 line-clamp-2 block max-w-md">
          {r.reasons}
        </span>
      ),
    },
  ];

  return (
    <div>
      <div className="flex items-baseline justify-between mb-4">
        <div>
          <h1 className="text-xl font-semibold">
            {routeDate ? `Historical scan` : "Today's Picks"}
          </h1>
          {data && (
            <p className="text-sm text-slate-400 mt-1">
              {data.date} — {data.total_count} candidates, {data.conflicts_count} conflicts
            </p>
          )}
        </div>
      </div>

      {loading && <LoadingSpinner />}
      {error && <div className="text-bear">{error}</div>}

      {data && (
        <>
          <div className="flex flex-wrap items-center gap-3 mb-4 text-sm">
            <input
              type="text"
              placeholder="Search ticker..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="bg-panel border border-slate-700 rounded px-3 py-1.5 text-slate-100 focus:outline-none focus:border-accent"
            />
            <select
              value={scannerFilter}
              onChange={(e) => setScannerFilter(e.target.value)}
              className="bg-panel border border-slate-700 rounded px-3 py-1.5 text-slate-100 focus:outline-none focus:border-accent"
            >
              <option value="all">All scanners</option>
              {allScanners.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
            <div className="flex gap-1">
              {(["all", "bull", "bear", "conflict"] as DirectionFilter[]).map((d) => (
                <button
                  key={d}
                  onClick={() => setDirectionFilter(d)}
                  className={`px-3 py-1.5 rounded border text-xs uppercase tracking-wide ${
                    directionFilter === d
                      ? "bg-accent text-ink border-accent"
                      : "bg-panel border-slate-700 text-slate-400 hover:text-slate-200"
                  }`}
                >
                  {d}
                </button>
              ))}
            </div>
            <span className="text-slate-500 text-xs ml-auto">
              {filtered.length} / {data.total_count} shown
            </span>
          </div>

          <DataTable
            rows={filtered}
            columns={columns}
            rowKey={(r) => r.ticker}
            onRowClick={(r) => navigate(`/ticker/${r.ticker}`)}
            initialSort={{ key: "score", dir: "desc" }}
            emptyMessage="No candidates match the current filters."
          />
        </>
      )}
    </div>
  );
}
