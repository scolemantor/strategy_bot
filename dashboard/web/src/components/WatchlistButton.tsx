// Phase 8c WatchlistButton — star toggle, three states, optimistic UI
// via the global watchlistStore. Color logic:
//   - not watched          → outlined star, slate-500
//   - watched, no position → filled star, slate-300
//   - watched, position>0  → filled star, bull (green)
//   - pending (mid-API)    → spinning + dim
//
// Stops row click event propagation so a parent <tr onClick> doesn't fire
// when the user clicks the star (Sean's row-hover-clickable pattern).

import { Star } from "lucide-react";
import type { MouseEvent } from "react";

import { useWatchlistStore } from "../stores/watchlistStore";

interface WatchlistButtonProps {
  ticker: string;
  size?: "sm" | "md" | "lg";
  showLabel?: boolean;
}

const SIZE_PX: Record<NonNullable<WatchlistButtonProps["size"]>, number> = {
  sm: 14,
  md: 18,
  lg: 24,
};

export function WatchlistButton({
  ticker,
  size = "md",
  showLabel = false,
}: WatchlistButtonProps) {
  const sym = ticker.toUpperCase();
  const isWatched = useWatchlistStore((s) => s.tickers.has(sym));
  const isPending = useWatchlistStore((s) => s.pendingTickers.has(sym));
  const entry = useWatchlistStore((s) =>
    s.entries.find((e) => e.ticker.toUpperCase() === sym),
  );
  const add = useWatchlistStore((s) => s.add);
  const remove = useWatchlistStore((s) => s.remove);

  // Phase 8c Issue B diagnostic — fires on every render
  console.log(
    `[WB] ${sym} isWatched=${isWatched} isPending=${isPending} hasEntry=${!!entry}`,
  );

  const hasPosition =
    entry?.position_size != null && entry.position_size > 0;

  const px = SIZE_PX[size];

  const colorClass = isPending
    ? "text-slate-500 opacity-50 animate-pulse"
    : !isWatched
      ? "text-slate-500 hover:text-slate-300"
      : hasPosition
        ? "text-bull hover:text-bull/80"
        : "text-slate-300 hover:text-slate-100";

  const tierLabel = entry ? `T${entry.tier}` : "";
  const aria = isWatched
    ? `Remove ${sym} from watchlist`
    : `Add ${sym} to watchlist`;
  const tooltip = isWatched
    ? hasPosition
      ? `Watching · ${tierLabel} · ${entry?.position_size} sh`
      : `Watching · ${tierLabel}`
    : "Add to watchlist";

  function handleClick(e: MouseEvent<HTMLButtonElement>) {
    e.preventDefault();
    e.stopPropagation();
    if (isPending) return;
    if (isWatched) {
      void remove(sym);
    } else {
      void add({ ticker: sym, source: "dashboard" });
    }
  }

  return (
    <button
      type="button"
      onClick={handleClick}
      disabled={isPending}
      aria-label={aria}
      aria-pressed={isWatched}
      title={tooltip}
      className={`inline-flex items-center gap-1.5 transition-colors ${colorClass}`}
    >
      <Star
        size={px}
        strokeWidth={1.75}
        fill={isWatched ? "currentColor" : "none"}
        aria-hidden
      />
      {showLabel && (
        <span className="text-xs">{isWatched ? "Watching" : "Watch"}</span>
      )}
    </button>
  );
}
