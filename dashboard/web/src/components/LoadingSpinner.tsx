export function LoadingSpinner({ label }: { label?: string }) {
  return (
    <div className="flex items-center gap-3 text-slate-400 text-sm">
      <div className="h-4 w-4 rounded-full border-2 border-slate-500 border-t-accent animate-spin" />
      {label ?? "Loading..."}
    </div>
  );
}
