import { NavLink, Outlet, useNavigate } from "react-router-dom";
import { api } from "../api";

const NAV = [
  { path: "/today", label: "Today" },
  { path: "/watchlist", label: "Watchlist" },
  { path: "/history", label: "History" },
  { path: "/notifications", label: "Notifications" },
  { path: "/settings", label: "Settings" },
];

export function Layout() {
  const navigate = useNavigate();

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
    </div>
  );
}
