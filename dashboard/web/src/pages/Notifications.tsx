import { useEffect, useState } from "react";
import { api } from "../api";
import { LoadingSpinner } from "../components/LoadingSpinner";
import type { NotificationItem, NotificationsResponse } from "../types";

const OUTCOME_FILTERS = ["all", "dispatched", "failed", "suppressed", "test_mode"] as const;
type OutcomeFilter = (typeof OUTCOME_FILTERS)[number];

const OUTCOME_STYLE: Record<string, string> = {
  dispatched: "bg-bull/20 text-bull border-bull/30",
  failed: "bg-bear/20 text-bear border-bear/30",
  suppressed: "bg-amber-500/20 text-amber-300 border-amber-500/30",
  test_mode: "bg-slate-500/20 text-slate-400 border-slate-500/30",
};

const PAGE_SIZE = 50;

export function Notifications() {
  const [data, setData] = useState<NotificationsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [outcome, setOutcome] = useState<OutcomeFilter>("all");
  const [page, setPage] = useState(0);
  const [expanded, setExpanded] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    const params = new URLSearchParams();
    params.set("limit", String(PAGE_SIZE));
    params.set("offset", String(page * PAGE_SIZE));
    if (outcome !== "all") params.set("outcome", outcome);
    api
      .get<NotificationsResponse>(`/api/notifications?${params.toString()}`)
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .catch(() => {
        if (!cancelled) setError("Failed to load notifications.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [outcome, page]);

  const totalPages = data ? Math.max(1, Math.ceil(data.total / PAGE_SIZE)) : 1;

  return (
    <div>
      <div className="flex items-baseline justify-between mb-4">
        <h1 className="text-xl font-semibold">Notifications</h1>
        {data && (
          <span className="text-sm text-slate-400">
            {data.total} total · page {page + 1} / {totalPages}
          </span>
        )}
      </div>

      <div className="flex gap-1 mb-4">
        {OUTCOME_FILTERS.map((o) => (
          <button
            key={o}
            onClick={() => {
              setOutcome(o);
              setPage(0);
            }}
            className={`px-3 py-1.5 rounded border text-xs uppercase tracking-wide ${
              outcome === o
                ? "bg-accent text-ink border-accent"
                : "bg-panel border-slate-700 text-slate-400 hover:text-slate-200"
            }`}
          >
            {o}
          </button>
        ))}
      </div>

      {loading && <LoadingSpinner />}
      {error && <div className="text-bear">{error}</div>}

      {data && (
        <>
          {data.items.length === 0 ? (
            <p className="text-slate-500 text-sm py-8 text-center">No notifications.</p>
          ) : (
            <ul className="space-y-1.5">
              {data.items.map((n) => (
                <NotificationRow
                  key={n.id}
                  n={n}
                  expanded={expanded === n.id}
                  onToggle={() => setExpanded(expanded === n.id ? null : n.id)}
                />
              ))}
            </ul>
          )}

          <div className="flex items-center justify-between mt-4 text-sm">
            <button
              onClick={() => setPage(Math.max(0, page - 1))}
              disabled={page === 0}
              className="px-3 py-1.5 rounded border border-slate-700 text-slate-300 hover:bg-panel2 disabled:opacity-40"
            >
              ← Previous
            </button>
            <button
              onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
              disabled={page >= totalPages - 1}
              className="px-3 py-1.5 rounded border border-slate-700 text-slate-300 hover:bg-panel2 disabled:opacity-40"
            >
              Next →
            </button>
          </div>
        </>
      )}
    </div>
  );
}

function NotificationRow({
  n,
  expanded,
  onToggle,
}: {
  n: NotificationItem;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <li className="bg-panel border border-slate-800 rounded">
      <button onClick={onToggle} className="w-full text-left px-3 py-2 hover:bg-panel2">
        <div className="flex items-center gap-3 text-sm">
          <span className="text-slate-500 tabular-nums text-xs whitespace-nowrap">
            {n.sent_at.replace("T", " ").slice(0, 19)}
          </span>
          <span
            className={`inline-flex px-2 py-0.5 rounded text-[10px] font-semibold border ${
              OUTCOME_STYLE[n.outcome] ?? OUTCOME_STYLE.test_mode
            }`}
          >
            {n.outcome}
            {n.suppression_reason ? ` · ${n.suppression_reason}` : ""}
          </span>
          <span className="text-slate-300 font-mono text-xs">{n.event_type}</span>
          <span className="text-slate-100 truncate flex-1">{n.title ?? n.message}</span>
        </div>
      </button>
      {expanded && (
        <div className="px-3 pb-3 pt-0 text-xs text-slate-300 whitespace-pre-wrap font-mono">
          {n.message}
        </div>
      )}
    </li>
  );
}
