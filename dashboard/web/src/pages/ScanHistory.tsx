import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api";
import { LoadingSpinner } from "../components/LoadingSpinner";
import type { HistoryEntry } from "../types";

export function ScanHistory() {
  const navigate = useNavigate();
  const [entries, setEntries] = useState<HistoryEntry[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    api
      .get<HistoryEntry[]>("/api/history")
      .then((d) => {
        if (!cancelled) setEntries(d);
      })
      .catch(() => {
        if (!cancelled) setError("Failed to load scan history.");
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (error) return <div className="text-bear">{error}</div>;
  if (entries === null) return <LoadingSpinner />;

  if (entries.length === 0) {
    return (
      <div>
        <h1 className="text-xl font-semibold mb-4">Scan History</h1>
        <p className="text-slate-500">No archived scans yet.</p>
      </div>
    );
  }

  return (
    <div>
      <h1 className="text-xl font-semibold mb-4">Scan History</h1>
      <p className="text-sm text-slate-400 mb-4">
        Last {entries.length} dates with scan output.
      </p>
      <div className="border border-slate-800 rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-panel2 text-slate-300 uppercase text-[11px]">
            <tr>
              <th className="px-3 py-2 text-left w-32">Date</th>
              <th className="px-3 py-2 text-left w-24">Candidates</th>
              <th className="px-3 py-2 text-left w-24">Scanners</th>
              <th className="px-3 py-2 text-left">Top 5</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {entries.map((e) => (
              <tr
                key={e.date}
                onClick={() => navigate(`/history/${e.date}`)}
                className="bg-panel cursor-pointer hover:bg-panel2"
              >
                <td className="px-3 py-2 text-slate-200 tabular-nums">{e.date}</td>
                <td className="px-3 py-2 text-slate-400 tabular-nums">
                  {e.candidate_count}
                </td>
                <td className="px-3 py-2 text-slate-400 tabular-nums">
                  {e.scanner_count}
                </td>
                <td className="px-3 py-2">
                  <div className="flex flex-wrap gap-1.5 text-xs">
                    {e.top_5.map((t) => (
                      <span
                        key={t}
                        className="bg-panel2 px-2 py-0.5 rounded text-slate-300 font-semibold"
                      >
                        {t}
                      </span>
                    ))}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
