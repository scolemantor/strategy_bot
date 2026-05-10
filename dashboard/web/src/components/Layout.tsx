import { useEffect } from "react";
import { NavLink, Outlet, useNavigate } from "react-router-dom";

import { api } from "../api";
import { useWatchlistStore } from "../stores/watchlistStore";
import { ToastContainer } from "./Toast";

const NAV = [
  // Phase 8c: "Today" link renamed to "Signals" (route also renamed
  // /today → /signals). The old /today path still resolves (redirects)
  // for muscle memory + bookmarks until we hard-remove in 8e.
  { path: "/signals", label: "Signals" },
  { path: "/watchlist", label: "Watchlist" },
  { path: "/history", label: "History" },
  { path: "/notifications", label: "Notifications" },
  { path: "/settings", label: "Settings" },
];

const POLL_INTERVAL_MS = 60_000;

export function Layout() {
  const navigate = useNavigate();
  const fetchWatchlist = useWatchlistStore((s) => s.fetchAll);

  // Phase 8c: poll the watchlist every 60s while the document is visible.
  // Pause on tab-hidden; resume + immediate refetch on tab-visible. The
  // store updates trigger re-renders for any subscribed component
  // (WatchlistButton, future Watchlist landing page, etc).
  useEffect(() => {
    let intervalId: ReturnType<typeof setInterval> | null = null;

    function startPolling() {
      // Immediate fetch + interval
      void fetchWatchlist();
      intervalId = setInterval(() => {
        void fetchWatchlist();
      }, POLL_INTERVAL_MS);
    }

    function stopPolling() {
      if (intervalId !== null) {
        clearInterval(intervalId);
        intervalId = null;
      }
    }

    function onVisibilityChange() {
      if (document.hidden) {
        stopPolling();
      } else if (intervalId === null) {
        startPolling();
      }
    }

    if (!document.hidden) {
      startPolling();
    }
    document.addEventListener("visibilitychange", onVisibilityChange);

    return () => {
      stopPolling();
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [fetchWatchlist]);

  async function handleLogout() {
    try {
      await api.post("/api/auth/logout");
    } catch {
      /* ignore — cookie still cleared client-side */
    }
    navigate("/login", { replace: true });
  }

  return (
    <div className="min-h-screen flex flex-col">
      <header className="border-b border-slate-800 bg-panel">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 flex items-center justify-between h-12">
          <div className="flex items-center gap-6">
            <span className="font-semibold text-accent">strategy_bot</span>
            <nav className="flex items-center gap-1 text-sm">
              {NAV.map((item) => (
                <NavLink
                  key={item.path}
                  to={item.path}
                  className={({ isActive }) =>
                    `px-3 py-1 rounded ${
                      isActive
                        ? "bg-panel2 text-slate-100"
                        : "text-slate-400 hover:text-slate-200 hover:bg-panel2/50"
                    }`
                  }
                >
                  {item.label}
                </NavLink>
              ))}
            </nav>
          </div>
          <button
            onClick={handleLogout}
            className="text-sm text-slate-400 hover:text-slate-200 px-3 py-1 rounded hover:bg-panel2/50"
          >
            Sign out
          </button>
        </div>
      </header>
      <main className="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 py-6">
        <Outlet />
      </main>
      {/* Toast container offset top-14 to clear the h-12 sticky header. */}
      <ToastContainer />
    </div>
  );
}
