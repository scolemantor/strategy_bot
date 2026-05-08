// Reusable sortable + filterable table.
// Generic over T (the row shape). Caller supplies columns + a key extractor.

import { ReactNode, useMemo, useState } from "react";

export interface Column<T> {
  key: string;
  header: string;
  // Either an accessor returning a primitive (used for sorting) ...
  accessor?: (row: T) => string | number | boolean | null | undefined;
  // ... and/or a render function (used for display).
  render?: (row: T) => ReactNode;
  // Width hint (Tailwind class)
  className?: string;
  sortable?: boolean;
}

export function DataTable<T>({
  rows,
  columns,
  rowKey,
  onRowClick,
  emptyMessage = "No rows.",
  initialSort,
}: {
  rows: T[];
  columns: Column<T>[];
  rowKey: (row: T) => string | number;
  onRowClick?: (row: T) => void;
  emptyMessage?: string;
  initialSort?: { key: string; dir: "asc" | "desc" };
}) {
  const [sort, setSort] = useState<{ key: string; dir: "asc" | "desc" } | null>(
    initialSort ?? null,
  );

  const sorted = useMemo(() => {
    if (!sort) return rows;
    const col = columns.find((c) => c.key === sort.key);
    if (!col?.accessor) return rows;
    const sign = sort.dir === "asc" ? 1 : -1;
    return [...rows].sort((a, b) => {
      const av = col.accessor!(a);
      const bv = col.accessor!(b);
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * sign;
      return String(av).localeCompare(String(bv)) * sign;
    });
  }, [rows, columns, sort]);

  function toggleSort(key: string, sortable: boolean | undefined) {
    if (!sortable) return;
    setSort((cur) => {
      if (!cur || cur.key !== key) return { key, dir: "desc" };
      if (cur.dir === "desc") return { key, dir: "asc" };
      return null;
    });
  }

  if (rows.length === 0) {
    return <div className="text-slate-500 text-sm py-8 text-center">{emptyMessage}</div>;
  }

  return (
    <div className="overflow-x-auto border border-slate-800 rounded-lg">
      <table className="w-full text-sm">
        <thead className="bg-panel2 text-slate-300 uppercase text-[11px] tracking-wide">
          <tr>
            {columns.map((c) => (
              <th
                key={c.key}
                onClick={() => toggleSort(c.key, c.sortable)}
                className={`px-3 py-2 text-left font-semibold ${
                  c.sortable ? "cursor-pointer hover:text-slate-100" : ""
                } ${c.className ?? ""}`}
              >
                <span className="inline-flex items-center gap-1">
                  {c.header}
                  {sort?.key === c.key && (
                    <span className="text-accent">{sort.dir === "asc" ? "▲" : "▼"}</span>
                  )}
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800">
          {sorted.map((row) => (
            <tr
              key={rowKey(row)}
              onClick={() => onRowClick?.(row)}
              className={`bg-panel ${
                onRowClick ? "cursor-pointer hover:bg-panel2" : ""
              }`}
            >
              {columns.map((c) => (
                <td key={c.key} className={`px-3 py-2 align-middle ${c.className ?? ""}`}>
                  {c.render ? c.render(row) : String(c.accessor?.(row) ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
