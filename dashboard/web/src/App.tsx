import { Navigate, Route, Routes } from "react-router-dom";
import { ProtectedRoute } from "./components/ProtectedRoute";
import { Layout } from "./components/Layout";
import { Login } from "./pages/Login";
import { Today } from "./pages/Today";
import { Watchlist } from "./pages/Watchlist";
import { TickerDetail } from "./pages/TickerDetail";
import { ScanHistory } from "./pages/ScanHistory";
import { Notifications } from "./pages/Notifications";
import { Settings } from "./pages/Settings";

export default function App() {
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
        <Route index element={<Navigate to="/today" replace />} />
        <Route path="/today" element={<Today />} />
        <Route path="/watchlist" element={<Watchlist />} />
        <Route path="/ticker/:symbol" element={<TickerDetail />} />
        <Route path="/history" element={<ScanHistory />} />
        <Route path="/history/:date" element={<Today />} />
        <Route path="/notifications" element={<Notifications />} />
        <Route path="/settings" element={<Settings />} />
      </Route>
      <Route path="*" element={<Navigate to="/today" replace />} />
    </Routes>
  );
}
