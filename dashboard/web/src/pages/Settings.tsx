import { useEffect, useState } from "react";
import { api } from "../api";
import { LoadingSpinner } from "../components/LoadingSpinner";
import type { ScannerSetting, SettingsResponse } from "../types";

interface AuditItem {
  changed_at: string;
  changed_by: string | null;
  scanner_name: string | null;
  field_name: string | null;
  old_value: string | null;
  new_value: string | null;
}

export function Settings() {
  const [data, setData] = useState<SettingsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [edits, setEdits] = useState<Record<string, { weight?: number; enabled?: boolean }>>({});
  const [savingName, setSavingName] = useState<string | null>(null);
  const [audit, setAudit] = useState<AuditItem[]>([]);
  const [toast, setToast] = useState<string | null>(null);

  async function reload() {
    const [settings, auditResp] = await Promise.all([
      api.get<SettingsResponse>("/api/settings"),
      api.get<{ items: AuditItem[] }>("/api/settings/audit"),
    ]);
    setData(settings);
    setAudit(auditResp.items);
  }

  useEffect(() => {
    reload()
      .catch(() => setError("Failed to load settings."))
      .finally(() => setLoading(false));
  }, []);

  function patchEdit(name: string, patch: Partial<{ weight: number; enabled: boolean }>) {
    setEdits((cur) => ({ ...cur, [name]: { ...cur[name], ...patch } }));
  }

  async function save(s: ScannerSetting) {
    const edit = edits[s.name];
    if (!edit) return;
    const body: { weight?: number; enabled?: boolean } = {};
    if (edit.weight != null && edit.weight !== s.weight) body.weight = edit.weight;
    if (edit.enabled != null && edit.enabled !== s.enabled) body.enabled = edit.enabled;
    if (!Object.keys(body).length) return;
    setSavingName(s.name);
    try {
      await api.put(`/api/settings/scanner/${s.name}`, body);
      await reload();
      setEdits((cur) => {
        const next = { ...cur };
        delete next[s.name];
        return next;
      });
      setToast(`Saved ${s.name}`);
      setTimeout(() => setToast(null), 2500);
    } catch {
      setToast(`Failed to save ${s.name}`);
      setTimeout(() => setToast(null), 4000);
    } finally {
      setSavingName(null);
    }
  }

  if (loading) return <LoadingSpinner />;
  if (error) return <div className="text-bear">{error}</div>;
  if (!data) return null;

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-xl font-semibold mb-2">Settings</h1>
        <p className="text-sm text-slate-400">
          Edits write directly to{" "}
          <code className="text-accent">config/scanner_weights.yaml</code>. Changes apply
          on the next scan.
        </p>
      </div>

      {toast && (
        <div className="fixed top-16 right-6 bg-panel2 border border-slate-700 rounded px-4 py-2 text-sm text-slate-100 shadow-lg">
          {toast}
        </div>
      )}

      <div className="border border-slate-800 rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-panel2 text-slate-300 uppercase text-[11px]">
            <tr>
              <th className="px-3 py-2 text-left">Scanner</th>
              <th className="px-3 py-2 text-left w-24">Enabled</th>
              <th className="px-3 py-2 text-left w-32">Weight</th>
              <th className="px-3 py-2 text-left w-24">Direction</th>
              <th className="px-3 py-2 text-left w-24">Category</th>
              <th className="px-3 py-2 text-left w-44">Last updated</th>
              <th className="px-3 py-2 text-right w-24">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {data.scanners.map((s) => {
              const edit = edits[s.name] ?? {};
              const enabled = edit.enabled ?? s.enabled;
              const weight = edit.weight ?? s.weight;
              const dirty =
                (edit.weight != null && edit.weight !== s.weight) ||
                (edit.enabled != null && edit.enabled !== s.enabled);
              return (
                <tr key={s.name} className="bg-panel">
                  <td className="px-3 py-2 font-mono text-slate-100">{s.name}</td>
                  <td className="px-3 py-2">
                    <button
                      onClick={() => patchEdit(s.name, { enabled: !enabled })}
                      className={`px-2.5 py-1 rounded text-xs font-semibold ${
                        enabled
                          ? "bg-bull/20 text-bull border border-bull/30"
                          : "bg-bear/20 text-bear border border-bear/30"
                      }`}
                    >
                      {enabled ? "ON" : "OFF"}
                    </button>
                  </td>
                  <td className="px-3 py-2">
                    <input
                      type="number"
                      step="0.1"
                      min="0"
                      max="5"
                      value={weight}
                      onChange={(e) =>
                        patchEdit(s.name, { weight: parseFloat(e.target.value) })
                      }
                      className="w-20 bg-ink border border-slate-700 rounded px-2 py-1 text-slate-100 text-sm focus:outline-none focus:border-accent"
                    />
                  </td>
                  <td className="px-3 py-2 text-slate-400">{s.direction}</td>
                  <td className="px-3 py-2 text-slate-400">{s.category}</td>
                  <td className="px-3 py-2 text-slate-400 text-xs">
                    {s.last_updated
                      ? `${s.last_updated.replace("T", " ").slice(0, 16)} (${s.last_updated_by ?? "?"})`
                      : "—"}
                  </td>
                  <td className="px-3 py-2 text-right">
                    <button
                      onClick={() => save(s)}
                      disabled={!dirty || savingName === s.name}
                      className="bg-accent text-ink px-3 py-1 rounded text-xs font-semibold hover:bg-accent/90 disabled:opacity-30"
                    >
                      {savingName === s.name ? "Saving..." : "Save"}
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div>
        <h2 className="text-sm uppercase tracking-wide text-slate-400 mb-2">
          Audit log (last 50 changes)
        </h2>
        {audit.length === 0 ? (
          <p className="text-slate-500 text-sm">No changes recorded yet.</p>
        ) : (
          <div className="border border-slate-800 rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-panel2 text-slate-300 uppercase text-[11px]">
                <tr>
                  <th className="px-3 py-2 text-left w-44">When</th>
                  <th className="px-3 py-2 text-left w-24">Who</th>
                  <th className="px-3 py-2 text-left">Scanner / Field</th>
                  <th className="px-3 py-2 text-left">Old → New</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {audit.map((a, i) => (
                  <tr key={`${a.changed_at}-${i}`} className="bg-panel">
                    <td className="px-3 py-2 text-slate-400 text-xs tabular-nums">
                      {a.changed_at.replace("T", " ").slice(0, 19)}
                    </td>
                    <td className="px-3 py-2 text-slate-400">{a.changed_by ?? "—"}</td>
                    <td className="px-3 py-2 text-slate-300 font-mono text-xs">
                      {a.scanner_name} / {a.field_name}
                    </td>
                    <td className="px-3 py-2 text-slate-400 font-mono text-xs">
                      {a.old_value} → <span className="text-slate-100">{a.new_value}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
