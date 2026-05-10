// Phase 8c global watchlist store (Zustand).
//
// Single source of truth for watchlist state across all components.
// WatchlistButton (and any future component) subscribes via useWatchlistStore
// and re-renders on store changes. Optimistic UI: add/remove mutate state
// immediately, then call API; on failure, snapshot is restored and a toast
// fires.
//
// Polling lives in Layout.tsx — every 60s if document is visible.

import { create } from "zustand";

import { api, ApiError } from "../api";
import { toast } from "../components/Toast";
import type {
  WatchlistAddRequest,
  WatchlistEntriesResponse,
  WatchlistEntry,
} from "../types";

interface WatchlistState {
  tickers: Set<string>;
  entries: WatchlistEntry[];
  lastTechnicalScan: string | null;
  loading: boolean;
  error: string | null;
  initialized: boolean;
  pendingTickers: Set<string>; // optimistic-update guards

  fetchAll: () => Promise<void>;
  add: (req: WatchlistAddRequest) => Promise<boolean>;
  remove: (ticker: string) => Promise<boolean>;
  update: (
    ticker: string,
    fields: Partial<WatchlistEntry>,
  ) => Promise<boolean>;
  isWatched: (ticker: string) => boolean;
  isPending: (ticker: string) => boolean;
  getEntry: (ticker: string) => WatchlistEntry | undefined;
}

function tickersFromEntries(entries: WatchlistEntry[]): Set<string> {
  return new Set(entries.map((e) => e.ticker.toUpperCase()));
}

export const useWatchlistStore = create<WatchlistState>((set, get) => ({
  tickers: new Set<string>(),
  entries: [],
  lastTechnicalScan: null,
  loading: false,
  error: null,
  initialized: false,
  pendingTickers: new Set<string>(),

  isWatched: (ticker) => get().tickers.has(ticker.toUpperCase()),
  isPending: (ticker) => get().pendingTickers.has(ticker.toUpperCase()),
  getEntry: (ticker) =>
    get().entries.find((e) => e.ticker.toUpperCase() === ticker.toUpperCase()),

  fetchAll: async () => {
    set({ loading: true, error: null });
    try {
      const resp = await api.get<WatchlistEntriesResponse>(
        "/api/watchlist/entries",
      );
      set({
        entries: resp.entries,
        tickers: tickersFromEntries(resp.entries),
        lastTechnicalScan: resp.last_technical_scan,
        loading: false,
        initialized: true,
      });
    } catch (e) {
      const msg = e instanceof ApiError ? `API ${e.status}` : String(e);
      set({ loading: false, error: msg, initialized: true });
      // Don't toast 401 — ProtectedRoute handles re-auth on next nav
      if (!(e instanceof ApiError && e.status === 401)) {
        toast.error(`Failed to load watchlist: ${msg}`);
      }
    }
  },

  add: async (req) => {
    const sym = req.ticker.toUpperCase();
    const snapshot = {
      entries: get().entries,
      tickers: get().tickers,
    };
    // Optimistic: add a placeholder entry immediately
    const placeholder: WatchlistEntry = {
      ticker: sym,
      tier: req.tier ?? 2,
      position_size: req.position_size ?? null,
      entry_price: req.entry_price ?? null,
      stop_loss: req.stop_loss ?? null,
      target_price: req.target_price ?? null,
      notes: req.notes ?? "",
      reason: req.reason ?? "",
      category: req.category ?? "general",
      auto_added: false,
      added_at: new Date().toISOString(),
      last_modified: new Date().toISOString(),
      added_date: "",
      latest_technicals: null,
    };
    const newPending = new Set(get().pendingTickers);
    newPending.add(sym);
    set({
      entries: [...snapshot.entries, placeholder],
      tickers: new Set([...snapshot.tickers, sym]),
      pendingTickers: newPending,
    });

    try {
      const created = await api.post<WatchlistEntry>(
        "/api/watchlist/entries",
        { ...req, ticker: sym, source: req.source ?? "dashboard" },
      );
      // Replace placeholder with server response
      const newEntries = get().entries.map((e) =>
        e.ticker.toUpperCase() === sym ? created : e,
      );
      const newPending2 = new Set(get().pendingTickers);
      newPending2.delete(sym);
      set({
        entries: newEntries,
        pendingTickers: newPending2,
      });
      toast.success(`Added ${sym} to watchlist`);
      return true;
    } catch (e) {
      // Rollback
      const newPending2 = new Set(get().pendingTickers);
      newPending2.delete(sym);
      set({
        entries: snapshot.entries,
        tickers: snapshot.tickers,
        pendingTickers: newPending2,
      });
      const msg =
        e instanceof ApiError
          ? e.status === 409
            ? `${sym} already on watchlist`
            : e.status === 401
              ? "Session expired"
              : `API ${e.status}`
          : String(e);
      toast.error(`Failed to add ${sym}: ${msg}`);
      return false;
    }
  },

  remove: async (ticker) => {
    const sym = ticker.toUpperCase();
    const snapshot = {
      entries: get().entries,
      tickers: get().tickers,
    };
    const newTickers = new Set(snapshot.tickers);
    newTickers.delete(sym);
    const newPending = new Set(get().pendingTickers);
    newPending.add(sym);
    set({
      entries: snapshot.entries.filter(
        (e) => e.ticker.toUpperCase() !== sym,
      ),
      tickers: newTickers,
      pendingTickers: newPending,
    });

    try {
      await api.delete<{ removed: boolean }>(
        `/api/watchlist/entries/${sym}?source=dashboard`,
      );
      const newPending2 = new Set(get().pendingTickers);
      newPending2.delete(sym);
      set({ pendingTickers: newPending2 });
      toast.success(`Removed ${sym} from watchlist`);
      return true;
    } catch (e) {
      const newPending2 = new Set(get().pendingTickers);
      newPending2.delete(sym);
      set({
        entries: snapshot.entries,
        tickers: snapshot.tickers,
        pendingTickers: newPending2,
      });
      const msg =
        e instanceof ApiError
          ? e.status === 404
            ? "not on watchlist"
            : e.status === 401
              ? "Session expired"
              : `API ${e.status}`
          : String(e);
      toast.error(`Failed to remove ${sym}: ${msg}`);
      return false;
    }
  },

  update: async (ticker, fields) => {
    const sym = ticker.toUpperCase();
    const snapshot = {
      entries: get().entries,
      tickers: get().tickers,
    };
    const idx = snapshot.entries.findIndex(
      (e) => e.ticker.toUpperCase() === sym,
    );
    if (idx < 0) {
      toast.error(`${sym} not on watchlist`);
      return false;
    }
    const optimistic = { ...snapshot.entries[idx], ...fields };
    const newEntries = [...snapshot.entries];
    newEntries[idx] = optimistic;
    const newPending = new Set(get().pendingTickers);
    newPending.add(sym);
    set({ entries: newEntries, pendingTickers: newPending });

    try {
      const updated = await api.put<WatchlistEntry>(
        `/api/watchlist/entries/${sym}?source=dashboard`,
        fields,
      );
      const finalEntries = [...get().entries];
      const finalIdx = finalEntries.findIndex(
        (e) => e.ticker.toUpperCase() === sym,
      );
      if (finalIdx >= 0) finalEntries[finalIdx] = updated;
      const newPending2 = new Set(get().pendingTickers);
      newPending2.delete(sym);
      set({ entries: finalEntries, pendingTickers: newPending2 });
      toast.success(`Updated ${sym}`);
      return true;
    } catch (e) {
      const newPending2 = new Set(get().pendingTickers);
      newPending2.delete(sym);
      set({
        entries: snapshot.entries,
        tickers: snapshot.tickers,
        pendingTickers: newPending2,
      });
      const msg = e instanceof ApiError ? `API ${e.status}` : String(e);
      toast.error(`Failed to update ${sym}: ${msg}`);
      return false;
    }
  },
}));
