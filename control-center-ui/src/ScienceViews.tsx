import { useCallback, useEffect, useMemo, useState } from "react";
import { api } from "./api";

type Navigate = (page: string) => void;

const display = (value: unknown) => value === null || value === undefined || value === "" ? "—" : typeof value === "object" ? JSON.stringify(value) : String(value);
const short = (value: unknown) => { const text = display(value); return text.length > 110 ? `${text.slice(0, 107)}…` : text; };

export function EcosystemPage({ navigate }: { navigate: Navigate }) {
  const [data, setData] = useState<any>(null); const [error, setError] = useState<string | null>(null);
  useEffect(() => { void api<any>("/api/science/ecosystem").then(setData).catch((cause) => setError(cause instanceof Error ? cause.message : "Unavailable")); }, []);
  const targets: Record<string, string> = { soil: "Data Soil", sensors: "Wallet Sensors", hypotheses: "Hypothesis Lab", indicators: "Indicator Forge", experiments: "Experiments", confidence: "Confidence Engine", risk: "Execution + Risk", watchers: "Watchers + Alerts", control: "Overview" };
  return <div className="science-layout">
    <section className="panel ecosystem span-8"><div className="panel-title"><div><span className="eyebrow">BEELZEBUB SCIENTIFIC LOOP</span><h2>Ecosystem map</h2><p>Wallets are sensors. Indicators are knowledge. Trades are experiments. Models are accumulated evidence.</p></div></div>
      {error ? <p className="empty-note">{error}</p> : <div className="ecosystem-map">{(data?.nodes || []).map((node: any) => <button key={node.id} className={`organism ${String(node.state || "").toLowerCase()}`} onClick={() => targets[node.id] && navigate(targets[node.id])}><span>{node.label}</span><small>{node.state || "Unavailable"}</small></button>)}</div>}
      <div className="science-cycle">{(data?.cycle || ["Unavailable"]).map((step: string, index: number) => <span key={step}>{step}{index < (data?.cycle || []).length - 1 && <b>↓</b>}</span>)}</div>
    </section>
    <ScienceRail health={data?.health} />
  </div>;
}

export function ScienceResourcePage({ endpoint, title, subtitle, columns, search }: { endpoint: string; title: string; subtitle: string; columns: string[]; search?: boolean }) {
  const [data, setData] = useState<any>(null); const [error, setError] = useState<string | null>(null); const [query, setQuery] = useState("");
  const load = useCallback(() => { const suffix = search && query ? `?search=${encodeURIComponent(query)}` : ""; void api<any>(`${endpoint}${suffix}`).then((result) => { setData(result); setError(null); }).catch((cause) => setError(cause instanceof Error ? cause.message : "Unavailable")); }, [endpoint, query, search]);
  useEffect(() => { load(); }, [load]);
  const shownColumns = columns.length ? columns : ["state", "database", "execution_mode", "storage"];
  const items = Array.isArray(data?.items) ? data.items : data ? [data] : [];
  return <section className="panel table-panel science-table"><div className="toolbar"><div><div><span className="eyebrow">SCIENTIFIC READ MODEL</span><h2>{title}</h2><p className="muted">{subtitle}</p></div>{search && <input aria-label="Search graveyard" placeholder="Search rejected hypotheses" value={query} onChange={(event) => setQuery(event.target.value)} />}</div><button className="button minor" onClick={load}>Refresh</button></div>
    {error ? <p className="empty-note science-message">Unavailable: {error}</p> : <div className="table-scroll"><table><thead><tr>{shownColumns.map((column) => <th key={column}>{column.replaceAll("_", " ")}</th>)}</tr></thead><tbody>{items.map((item: any, index: number) => <tr key={item.id || item.wallet || item.hypothesis_id || item.experiment_id || item.indicator_id || item.model_id || item.decision_id || index}>{shownColumns.map((column) => <td key={column} title={display(item[column])}>{short(item[column])}</td>)}</tr>)}{!items.length && <tr><td colSpan={shownColumns.length} className="empty">{data?.empty_state || "No evidence"}</td></tr>}</tbody></table></div>}
  </section>;
}

export function ConfidencePage() {
  const [data, setData] = useState<any>(null); const [error, setError] = useState<string | null>(null);
  useEffect(() => { void api<any>("/api/confidence").then(setData).catch((cause) => setError(cause instanceof Error ? cause.message : "Unavailable")); }, []);
  const items = data?.items || [];
  const points = useMemo(() => items.slice().reverse().map((item: any) => Number(item.payload?.effective_confidence)).filter((item: number) => Number.isFinite(item)), [items]);
  const path = useMemo(() => points.length < 2 ? "" : points.map((point: number, index: number) => `${index ? "L" : "M"}${index / (points.length - 1) * 100} ${95 - point * 90}`).join(" "), [points]);
  return <div className="science-layout"><section className="panel span-8"><div className="panel-title"><div><span className="eyebrow">CALIBRATED NOWCAST</span><h2>Confidence Engine</h2><p>Model confidence is evidence quality; trade confidence is the current calibrated net-positive probability; effective confidence shrinks uncertainty toward 0.5.</p></div></div>{error ? <p className="empty-note">Unavailable: {error}</p> : <><div className="confidence-chart">{path ? <svg viewBox="0 0 100 100" preserveAspectRatio="none"><path d={path} fill="none" stroke="var(--gold-bright)" strokeWidth="1.5" vectorEffect="non-scaling-stroke" /></svg> : <p className="empty-note">{data?.empty_state || "No evidence"}</p>}</div><div className="table-scroll"><table><thead><tr><th>Symbol</th><th>Model confidence</th><th>Trade confidence</th><th>Effective confidence</th><th>Expected net edge</th><th>Alpha survival</th><th>Decision</th></tr></thead><tbody>{items.map((item: any) => <tr key={item.decision_id}><td>{item.symbol}</td><td>{display(item.payload?.model_confidence)}</td><td>{display(item.payload?.trade_confidence)}</td><td>{display(item.payload?.effective_confidence)}</td><td>{display(item.payload?.expected_net_edge)}</td><td>{display(item.payload?.alpha_survival)}</td><td>{item.decision}</td></tr>)}{!items.length && <tr><td colSpan={7} className="empty">{data?.empty_state || "No evidence"}</td></tr>}</tbody></table></div></>}</section><ScienceRail /></div>;
}

export function ScienceRail({ health }: { health?: any }) {
  const [data, setData] = useState<any>(health || null);
  useEffect(() => { if (!health) void api<any>("/api/science/health").then(setData).catch(() => setData(null)); }, [health]);
  const storage = data?.storage || {};
  return <aside className="science-rail"><section className="panel"><span className="eyebrow">ECOSYSTEM HEALTH</span><h2>{data?.state || "Unavailable"}</h2><dl className="metric-list"><div><dt>Mode</dt><dd>{data?.execution_mode || "Unavailable"}</dd></div><div><dt>Cold archive</dt><dd>{storage.state || "Unavailable"}</dd></div><div><dt>Hot spool</dt><dd>{storage.spool ? `${storage.spool.files} files` : "—"}</dd></div><div><dt>Forward predictions</dt><dd>{data?.counts?.science_forward_predictions ?? "—"}</dd></div><div><dt>Risk state</dt><dd>Simulation only</dd></div></dl></section><section className="panel"><span className="eyebrow">NON-NEGOTIABLE</span><p className="muted">No UI action can arm live capital movement. Raw wallet observations require the validated indicator → model → edge → risk path.</p></section></aside>;
}
