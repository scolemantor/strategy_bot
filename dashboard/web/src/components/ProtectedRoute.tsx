import { useEffect, useState } from "react";
import { Navigate, useLocation } from "react-router-dom";
import { api, ApiError } from "../api";
import type { User } from "../types";
import { LoadingSpinner } from "./LoadingSpinner";

type AuthState =
  | { kind: "loading" }
  | { kind: "authed"; user: User }
  | { kind: "anon" };

export function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<AuthState>({ kind: "loading" });
  const location = useLocation();

  useEffect(() => {
    let cancelled = false;
    api
      .get<User>("/api/auth/me")
      .then((user) => {
        if (!cancelled) setState({ kind: "authed", user });
      })
      .catch((err) => {
        if (cancelled) return;
        if (err instanceof ApiError && err.status === 401) {
          setState({ kind: "anon" });
        } else {
          setState({ kind: "anon" });
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (state.kind === "loading") {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner label="Checking session..." />
      </div>
    );
  }
  if (state.kind === "anon") {
    return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  }
  return <>{children}</>;
}
