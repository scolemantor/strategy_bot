// Phase 8c WatchlistButton — Gmail-style star toggle, optimistic UI
// via the global watchlistStore. Two visual states:
//   - not watched    → outlined star, muted slate
//   - watched        → filled star, warm amber/gold (regardless of tier)
//   - pending (mid-API) modifier → existing color + opacity dim + pulse,
//     so the optimistic click flips to amber immediately rather than
//     blanking to gray
//
// Tier-based color variants are deferred to Phase 8e when tier badges ship.
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

  const px = SIZE_PX[size];

  // Watched = warm amber gold (Gmail-starred). Unwatched = muted slate.
  // Pending overlays opacity+pulse without overriding the color so the
  // optimistic click visibly flips immediately.
  const baseColor = isWatched
    ? "text-amber-400 hover:text-amber-300"
    : "text-slate-500 hover:text-slate-300";
  const pendingMod = isPending ? "opacity-50 animate-pulse" : "";
  const colorClass = [baseColor, pendingMod].filter(Boolean).join(" ");

  const tierLabel = entry ? `T${entry.tier}` : "";
  const hasPosition =
    entry?.position_size != null && entry.position_size > 0;
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
