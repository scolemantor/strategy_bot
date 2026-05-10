// Minimal toast system. Module-level subscribe-able list; ToastContainer
// renders all active toasts top-right (offset top-14 to clear the h-12
// sticky header in Layout). Auto-dismiss after 3s. ~50 LOC vs adding a
// dep — easy to swap to `sonner` later if we need stacks/severity/actions.

import { useEffect, useState } from "react";

export type ToastKind = "success" | "error" | "info";

export interface ToastItem {
  id: number;
  kind: ToastKind;
  message: string;
}

let nextId = 1;
let toasts: ToastItem[] = [];
const subscribers = new Set<() => void>();

function notify() {
  for (const fn of subscribers) fn();
}

function dismiss(id: number) {
  toasts = toasts.filter((t) => t.id !== id);
  notify();
}

function show(kind: ToastKind, message: string, durationMs = 3000) {
  const id = nextId++;
  toasts = [...toasts, { id, kind, message }];
  notify();
  setTimeout(() => dismiss(id), durationMs);
}

export const toast = {
  success: (msg: string) => show("success", msg),
  error: (msg: string) => show("error", msg, 5000),
  info: (msg: string) => show("info", msg),
};

export function useToast() {
  return toast;
}

export function ToastContainer() {
  const [, force] = useState(0);

  useEffect(() => {
    const cb = () => force((n) => n + 1);
    subscribers.add(cb);
    return () => {
      subscribers.delete(cb);
    };
  }, []);

  if (toasts.length === 0) return null;
  return (
    <div
      className="fixed top-14 right-4 z-50 flex flex-col gap-2 pointer-events-none"
      role="region"
      aria-live="polite"
    >
      {toasts.map((t) => (
        <div
          key={t.id}
          onClick={() => dismiss(t.id)}
          className={`pointer-events-auto cursor-pointer max-w-sm px-4 py-2.5 rounded-md text-sm shadow-lg border transition-all ${
            t.kind === "success"
              ? "bg-bull/10 border-bull/30 text-bull"
              : t.kind === "error"
                ? "bg-bear/10 border-bear/30 text-bear"
                : "bg-panel2 border-slate-700 text-slate-200"
          }`}
        >
          {t.message}
        </div>
      ))}
    </div>
  );
}
