import { useEffect } from "react";
import { Navigate, Route, Routes } from "react-router-dom";
import { ProtectedRoute } from "./components/ProtectedRoute";
import { Layout } from "./components/Layout";
import { Login } from "./pages/Login";
import { Signals } from "./pages/Signals";
import { Watchlist } from "./pages/Watchlist";
import { TickerDetail } from "./pages/TickerDetail";
import { ScanHistory } from "./pages/ScanHistory";
import { Notifications } from "./pages/Notifications";
import { Settings } from "./pages/Settings";
import { useWatchlistStore } from "./stores/watchlistStore";

// Phase 8c routing changes:
//   /          → temporarily redirects to /signals (will become Watchlist
//                landing in 8d)
//   /signals   → renamed from /today; renders the master_ranked discovery
//                feed (was the previous landing page)
//   /today     → kept as alias to /signals for muscle memory + bookmarks;
//                will be hard-removed in 8e once we've confirmed nobody's
//                hitting it
//   /watchlist → legacy digest CSV view, kept as redirect-only target;
//                will be hard-removed AFTER 8e once new Watchlist landing
//                is stable for a few days (Sean's conservative ask)
export default function App() {
  // Phase 8c Issue B diagnostic — log initial Zustand store state on App mount
  useEffect(() => {
    const s = useWatchlistStore.getState();
    console.log(
      `[App.mount] watchlistStore initial: ` +
        `initialized=${s.initialized} loading=${s.loading} ` +
        `entries=${s.entries.length} tickers=${[...s.tickers].join(",")} ` +
        `error=${s.error ?? "null"}`,
    );
  }, []);

  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route
        element={
          <ProtectedRoute>
            <Layout />
          </ProtectedRoute>
        }
      >
        <Route index element={<Navigate to="/signals" replace />} />
        <Route path="/signals" element={<Signals />} />
        <Route path="/today" element={<Navigate to="/signals" replace />} />
        <Route path="/watchlist" element={<Watchlist />} />
        <Route path="/ticker/:symbol" element={<TickerDetail />} />
        <Route path="/history" element={<ScanHistory />} />
        <Route path="/history/:date" element={<Signals />} />
        <Route path="/notifications" element={<Notifications />} />
        <Route path="/settings" element={<Settings />} />
      </Route>
      <Route path="*" element={<Navigate to="/signals" replace />} />
    </Routes>
  );
}
