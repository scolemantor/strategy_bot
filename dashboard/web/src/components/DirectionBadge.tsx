export function DirectionBadge({
  directions,
  isConflict = false,
}: {
  directions: string;
  isConflict?: boolean;
}) {
  if (isConflict) {
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-conflict/20 text-conflict border border-conflict/30">
        CONFLICT
      </span>
    );
  }
  const dirs = directions.split(",").map((d) => d.trim()).filter(Boolean);
  if (dirs.includes("bullish") && !dirs.includes("bearish")) {
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-bull/20 text-bull border border-bull/30">
        BULL
      </span>
    );
  }
  if (dirs.includes("bearish") && !dirs.includes("bullish")) {
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-bear/20 text-bear border border-bear/30">
        BEAR
      </span>
    );
  }
  return (
    <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-slate-500/20 text-slate-400 border border-slate-500/30">
      NEUTRAL
    </span>
  );
}
