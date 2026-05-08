// Visualizes a composite score as a horizontal bar. Width relative to the
// max in the dataset (or absolute if max omitted).

export function ScoreBar({
  score,
  max,
}: {
  score: number;
  max?: number;
}) {
  const cap = max ?? 5;
  const pct = Math.max(0, Math.min(100, (score / cap) * 100));
  const tone =
    score >= 4 ? "bg-bull" : score >= 2 ? "bg-accent" : score < 0 ? "bg-bear" : "bg-slate-500";
  return (
    <div className="flex items-center gap-2 min-w-[120px]">
      <div className="flex-1 h-2 bg-panel2 rounded overflow-hidden">
        <div className={`h-full ${tone}`} style={{ width: `${pct}%` }} />
      </div>
      <span className="font-mono text-xs text-slate-300 tabular-nums w-12 text-right">
        {score.toFixed(2)}
      </span>
    </div>
  );
}
