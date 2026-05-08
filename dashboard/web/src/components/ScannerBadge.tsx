// Color-coded pill per scanner category. Maps the scanner_name to its
// category color via a static lookup (kept in sync with scanner_weights.yaml
// categories).
const CATEGORY_COLORS: Record<string, string> = {
  conviction: "bg-purple-500/20 text-purple-300 border-purple-500/30",
  technical: "bg-blue-500/20 text-blue-300 border-blue-500/30",
  event: "bg-amber-500/20 text-amber-300 border-amber-500/30",
  value: "bg-emerald-500/20 text-emerald-300 border-emerald-500/30",
  default: "bg-slate-500/20 text-slate-300 border-slate-500/30",
};

const SCANNER_TO_CATEGORY: Record<string, string> = {
  insider_buying: "conviction",
  thirteen_f_changes: "conviction",
  insider_selling_clusters: "conviction",
  spinoff_tracker: "conviction",
  congressional_trades: "conviction",
  breakout_52w: "technical",
  earnings_drift: "technical",
  short_squeeze: "technical",
  sector_rotation: "technical",
  fda_calendar: "event",
  earnings_calendar: "event",
  macro_calendar: "event",
  ipo_lockup: "event",
  small_cap_value: "value",
};

export function ScannerBadge({ name }: { name: string }) {
  const cat = SCANNER_TO_CATEGORY[name] ?? "default";
  const cls = CATEGORY_COLORS[cat] ?? CATEGORY_COLORS.default;
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-mono border ${cls}`}
    >
      {name}
    </span>
  );
}
