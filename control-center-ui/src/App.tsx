import { useCallback, useEffect, useMemo, useState } from "react";
import { api, post } from "./api";
import type { Candidate, CandidatesResponse, ControlState, Portfolio } from "./types";
import { AutomatedSciencePage, ConfidencePage, DataIgnitionPage, EcosystemPage, ScienceResourcePage } from "./ScienceViews";
import { SchedulerPage } from "./SchedulerPage";

type Page = "Automated Science" | "Data Ignition" | "Ecosystem" | "Data Soil" | "Wallet Sensors" | "Hypothesis Lab" | "Indicator Forge" | "Experiments" | "Confidence Engine" | "Execution + Risk" | "Lane III Paper" | "Task Scheduler" | "Watchers + Alerts" | "Graveyard" | "Overview" | "Discovery" | "Candidates" | "Shadow" | "Active" | "Portfolio" | "Positions" | "Activity" | "System";
const pages: Page[] = ["Automated Science", "Data Ignition", "Ecosystem", "Data Soil", "Wallet Sensors", "Hypothesis Lab", "Indicator Forge", "Experiments", "Confidence Engine", "Execution + Risk", "Lane III Paper", "Task Scheduler", "Watchers + Alerts", "Graveyard", "Overview", "Discovery", "Candidates", "Shadow", "Active", "Portfolio", "Positions", "Activity", "System"];

const money = (value: unknown) => new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(Number(value || 0));
const percent = (value: unknown) => `${(Number(value || 0) * 100).toFixed(1)}%`;
const number = (value: unknown, digits = 1) => value === null || value === undefined ? "—" : Number(value).toFixed(digits);
const walletLabel = (wallet: string) => wallet.length > 14 ? `${wallet.slice(0, 8)}…${wallet.slice(-6)}` : wallet;
const timeLabel = (value?: string | null) => value ? new Date(value).toLocaleString() : "—";
const bytes = (value: unknown) => {
  const size = Number(value || 0);
  return size >= 1024 ** 3 ? `${(size / 1024 ** 3).toFixed(1)} GiB` : `${(size / 1024 ** 2).toFixed(1)} MiB`;
};
const optionalBytes = (value: unknown) => value === null || value === undefined ? "—" : bytes(value);

type Toast = { tone: "error" | "success" | "warning"; message: string } | null;
type Confirmation = { title: string; body: string; action: () => Promise<void>; confirm: string } | null;
const terminalDiscoveryStatuses = new Set(["completed", "completed_with_warnings", "failed", "cancelled"]);
const isTerminalDiscoveryJob = (status?: string) => terminalDiscoveryStatuses.has(String(status || ""));

export function App() {
  const [page, setPage] = useState<Page>("Overview");
  const [overview, setOverview] = useState<Record<string, any> | null>(null);
  const [portfolio, setPortfolio] = useState<Portfolio | null>(null);
  const [control, setControl] = useState<ControlState | null>(null);
  const [livePositions, setLivePositions] = useState<any[] | null>(null);
  const [watcherHealth, setWatcherHealth] = useState<Record<string, any> | null>(null);
  const [liveActivity, setLiveActivity] = useState<any[] | null>(null);
  const [discoveryJob, setDiscoveryJob] = useState<Record<string, any> | null>(null);
  const [discoveryRevision, setDiscoveryRevision] = useState(0);
  const [schedulerRevision, setSchedulerRevision] = useState(0);
  const [toast, setToast] = useState<Toast>(null);
  const [confirmation, setConfirmation] = useState<Confirmation>(null);

  const reportError = useCallback((error: unknown) => setToast({ tone: "error", message: error instanceof Error ? error.message : "The action failed. No state change was made." }), []);
  const refresh = useCallback(async () => {
    try {
      const [nextOverview, nextPortfolio, nextControl] = await Promise.all([api<Record<string, any>>("/api/overview"), api<Portfolio>("/api/portfolio"), api<ControlState>("/api/controls")]);
      setOverview(nextOverview); setPortfolio(nextPortfolio); setControl(nextControl);
    } catch (error) { reportError(error); }
  }, [reportError]);

  useEffect(() => { void refresh(); }, [refresh]);
  useEffect(() => {
    const ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`);
    ws.onmessage = (event) => {
      const message = JSON.parse(event.data) as { type: string; data: any };
      if (message.type === "portfolio_update") setPortfolio(message.data);
      if (message.type === "control_state") setControl(message.data);
      if (message.type === "position_update") setLivePositions(message.data?.items || []);
      if (message.type === "watcher_health") setWatcherHealth(message.data || null);
      if (message.type === "activity") setLiveActivity(message.data?.items || []);
      if (message.type === "discovery_job_update") {
        setDiscoveryJob(message.data || null);
        if (["completed", "completed_with_warnings"].includes(message.data?.status)) { setDiscoveryRevision((value) => value + 1); void refresh(); }
      }
      if (["scheduler_status", "scheduler_schedule_update", "scheduler_run_update", "scheduler_notification"].includes(message.type)) setSchedulerRevision((value) => value + 1);
    };
    ws.onerror = () => undefined;
    return () => ws.close();
  }, [refresh]);

  const command = async (path: string, success: string) => {
    try { const result = await post<ControlState & { control?: ControlState }>(path); setControl(result.control || result); setToast({ tone: "success", message: success }); await refresh(); }
    catch (error) { reportError(error); }
  };
  const closeAll = async (path: string, success: string) => {
    try {
      const result = await post<any>(path);
      setControl(result.control || result);
      if (result.status === "partial") {
        const remaining = result.remaining_open_positions?.length ?? 0;
        setToast({ tone: "warning", message: `Partial close-all: ${result.closed?.length || 0} groups closed; ${remaining} PAPER position${remaining === 1 ? " remains" : "s remain"} open. New PAPER entries remain paused.` });
      }
      else setToast({ tone: "success", message: success });
      await refresh();
    } catch (error) { reportError(error); }
  };

  return <div className="shell">
    <aside className="sidebar">
      <div className="brand"><span className="brand-mark">B</span><div><strong>BEELZEBUB</strong><small>SCIENTIFIC ALPHA ENGINE</small></div></div>
      <div className="mode-stamp">SIMULATION / SHADOW ONLY</div>
      <nav aria-label="Primary navigation">{pages.map((item) => <button key={item} className={page === item ? "nav-item selected" : "nav-item"} aria-current={page === item ? "page" : undefined} onClick={() => setPage(item)}>{item}</button>)}</nav>
      <div className="sidebar-foot"><span className="status-dot good" /> Local research terminal<br />No live execution capability</div>
    </aside>
    <main className="main">
      <header className="topbar">
        <div><div className="eyebrow">SCIENTIFIC ALPHA OPERATIONS</div><h1>{page}</h1></div>
        <div className="header-controls">
          <span className={`control-chip ${control?.entries_allowed ? "running" : "paused"}`}>{control?.state || "CONNECTING"}</span>
          {control?.entries_allowed
            ? <button className="button warning" onClick={() => void command("/api/controls/pause-entries", "New PAPER entries are paused. Existing exits remain enabled.")}>Pause Paper Entries</button>
            : <button className="button positive" onClick={() => void command("/api/controls/resume-entries", "New PAPER entries are enabled.")}>Resume Paper Entries</button>}
          <button className="button critical" onClick={() => setConfirmation({ title: "Close all PAPER positions?", body: "This will flatten every current paper sleeve using the latest valid market reference. Trader statuses will not change. If any sleeve cannot be closed, new PAPER entries stay paused.", confirm: "Close All Paper Positions", action: async () => { await closeAll("/api/controls/close-all-paper-positions", "Close-all PAPER action completed."); } })}>Close All Paper Positions</button>
          <button className="button critical outline" onClick={() => setConfirmation({ title: "Exit + pause PAPER trading?", body: "This will first flatten every current PAPER sleeve using valid market references, then disable new PAPER entries. Trader statuses will not change.", confirm: "Exit + Pause Paper Trading", action: async () => { await closeAll("/api/controls/exit-and-pause", "PAPER exit + pause action completed."); } })}>Exit + Pause</button>
        </div>
      </header>
      <section className="paper-banner"><strong>PAPER POSITIONS ONLY</strong><span>Research and simulated execution only. No credentials, signing, order submission, or live-mode controls exist in this application.</span></section>
      {page === "Ecosystem" && <EcosystemPage navigate={(target) => setPage(target as Page)} />}
      {page === "Automated Science" && <AutomatedSciencePage />}
      {page === "Data Ignition" && <DataIgnitionPage />}
      {page === "Data Soil" && <ScienceResourcePage endpoint="/api/science/health" title="Data Soil / Provenance" subtitle="Feature, database, storage, and archival evidence health." columns={[]} />}
      {page === "Wallet Sensors" && <ScienceResourcePage endpoint="/api/wallet-sensors" title="Wallet Sensors" subtitle="Wallets are behavioral sensors, never direct execution authorities." columns={["wallet", "evidence_confidence", "updated_at", "metrics"]} />}
      {page === "Hypothesis Lab" && <ScienceResourcePage endpoint="/api/hypotheses" title="Hypothesis Lab" subtitle="Immutable falsifiable propositions and declared test conditions." columns={["hypothesis_id", "version", "state", "registered_at", "config_hash", "definition"]} />}
      {page === "Indicator Forge" && <ScienceResourcePage endpoint="/api/indicators" title="Indicator Forge" subtitle="Validated relationships with provenance, regimes, decay, and evidence state." columns={["indicator_id", "version", "state", "created_at", "provenance"]} />}
      {page === "Experiments" && <ScienceResourcePage endpoint="/api/experiments" title="Experiments" subtitle="Historical and forward evidence remain separate, cost-adjusted, and reproducible." columns={["experiment_id", "kind", "state", "hypothesis_id", "dataset_fingerprint", "result"]} />}
      {page === "Confidence Engine" && <ConfidencePage />}
      {page === "Execution + Risk" && <ScienceResourcePage endpoint="/api/decisions" title="Execution + Risk" subtitle="Explainable simulation/shadow decisions after model, edge, and risk gates." columns={["created_at", "symbol", "decision", "payload"]} />}
      {page === "Lane III Paper" && <LaneIIIPaperPage notify={setToast} confirmation={setConfirmation} />}
      {page === "Task Scheduler" && <SchedulerPage revision={schedulerRevision} notify={setToast} confirmation={setConfirmation} />}
      {page === "Watchers + Alerts" && <ScienceResourcePage endpoint="/api/science/health" title="Watchers + Alerts" subtitle="Operational data is unavailable until real watcher evidence is persisted." columns={[]} />}
      {page === "Graveyard" && <ScienceResourcePage endpoint="/api/graveyard" title="Graveyard" subtitle="Rejected hypotheses are permanent evidence and searchable before rediscovery." columns={["hypothesis_id", "version", "reason", "recorded_at", "payload"]} search />}
      {page === "Overview" && <Overview data={overview} portfolio={portfolio} navigate={setPage} />}
      {page === "Discovery" && <DiscoveryPage discoveryJob={discoveryJob} navigate={setPage} confirmation={setConfirmation} refresh={refresh} />}
      {page === "Candidates" && <CandidatesPage key={discoveryRevision} notify={setToast} confirmation={setConfirmation} refresh={refresh} navigate={setPage} />}
      {page === "Shadow" && <ShadowPage notify={setToast} confirmation={setConfirmation} refresh={refresh} />}
      {page === "Active" && <ActivePage notify={setToast} confirmation={setConfirmation} refresh={refresh} />}
      {page === "Portfolio" && <PortfolioPage portfolio={portfolio} />}
      {page === "Positions" && <PositionsPage livePositions={livePositions} />}
      {page === "Activity" && <ActivityPage liveItems={liveActivity} />}
      {page === "System" && <><SystemPage control={control} watcherHealth={watcherHealth} /><ShadowObservationStatus /></>}
    </main>
    {toast && <div className={`toast ${toast.tone}`} role="alert"><span>{toast.tone === "error" ? "Action failed" : toast.tone === "warning" ? "Warning" : "Updated"}</span>{toast.message}<button onClick={() => setToast(null)} aria-label="Dismiss notification">×</button></div>}
    {confirmation && <ConfirmationDialog item={confirmation} close={() => setConfirmation(null)} />}
  </div>;
}

function Overview({ data, portfolio, navigate }: { data: Record<string, any> | null; portfolio: Portfolio | null; navigate: (page: Page) => void }) {
  const counts = data?.counts || {};
  const cards = [
    ["Discovered", counts.total_discovered, "Research universe"], ["Qualified", counts.qualified, "Current Phase B evidence"],
    ["Shadow", counts.shadow, "Monitored finalists"], ["Active", counts.active, "PAPER cohort"],
    ["Open PAPER P&L", money(portfolio?.open_pnl), "Unrealized"], ["Paper equity", money(portfolio?.equity), `Max DD ${percent(portfolio?.max_drawdown)}`],
  ];
  return <div className="page-grid">
    {Number(counts.total_discovered || 0) === 0 && <section className="panel span-12"><PanelTitle title="Candidate universe not initialized" subtitle="Start with recent public HyperCore activity, then use Phase B research before any manual PAPER cohort decision." /><button className="button positive" onClick={() => navigate("Discovery")}>Start Discovery</button></section>}
    <section className="stat-grid">{cards.map(([label, value, sub]) => <article className="panel stat" key={String(label)}><span>{label}</span><strong>{value ?? "—"}</strong><small>{sub}</small></article>)}</section>
    <section className="panel span-8"><PanelTitle title="Research funnel" action="Open candidates" onAction={() => navigate("Candidates")} /><div className="funnel">{(data?.funnel || []).map((stage: any) => <button key={stage.key} onClick={() => navigate("Candidates")}><strong>{Number(stage.count).toLocaleString()}</strong><span>{stage.label}</span></button>)}</div></section>
    <section className="panel span-4"><PanelTitle title="Paper portfolio" action="Open portfolio" onAction={() => navigate("Portfolio")} /><MetricList values={[["Free cash", money(portfolio?.cash)], ["Committed", money(portfolio?.committed_capital)], ["Realized total", money(portfolio?.realized_pnl_total)], ["Open positions", portfolio?.open_positions ?? "—"]]} /></section>
    <section className="panel span-7"><PanelTitle title="Top research candidates" action="Candidate table" onAction={() => navigate("Candidates")} /><CandidateRows candidates={data?.top_candidates || []} /></section>
    <section className="panel span-5"><PanelTitle title="Recent activity" action="Full activity" onAction={() => navigate("Activity")} /><ActivityList items={data?.recent_activity || []} /></section>
  </div>;
}

function DiscoveryPage({ discoveryJob, navigate, confirmation, refresh }: { discoveryJob: Record<string, any> | null; navigate: (page: Page) => void; confirmation: (value: Confirmation) => void; refresh: () => Promise<void> }) {
  const [data, setData] = useState<Record<string, any> | null>(null);
  const [selectedPreset, setSelectedPreset] = useState("standard");
  const [limitOverride, setLimitOverride] = useState("");
  const [activityOverride, setActivityOverride] = useState("");
  const [windowOverride, setWindowOverride] = useState("");
  const [ageOverride, setAgeOverride] = useState("");
  const [trackedJobId, setTrackedJobId] = useState<string | null>(null);
  const [polledJob, setPolledJob] = useState<Record<string, any> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [testing, setTesting] = useState(false);
  const load = useCallback(async () => { try { setData(await api("/api/discovery/status")); setError(null); } catch (cause) { setError(cause instanceof Error ? cause.message : "Unable to load discovery status."); } }, []);
  useEffect(() => { void load(); }, [load]);
  const source = data?.source || {}; const job = trackedJobId ? polledJob : discoveryJob || data?.current_job;
  const presets = data?.presets || { quick: { window_hours: 1, candidate_limit: 1000, min_activity: 2, max_activity_age: "30d" }, standard: { window_hours: 6, candidate_limit: 2500, min_activity: 2, max_activity_age: "30d" }, deep: { window_hours: 24, candidate_limit: 5000, min_activity: 2, max_activity_age: "30d" } };
  const sourceReady = source.connection_state === "READY";
  const running = ["queued", "acquiring", "parsing", "discovering"].includes(job?.status);
  useEffect(() => {
    if (!trackedJobId || discoveryJob?.job_id !== trackedJobId) return;
    setPolledJob((current) => current && isTerminalDiscoveryJob(current.status) && !isTerminalDiscoveryJob(discoveryJob.status) ? current : discoveryJob);
  }, [discoveryJob, trackedJobId]);
  useEffect(() => {
    if (!trackedJobId) return;
    let active = true;
    let timer: number | undefined;
    const poll = async () => {
      try {
        const current = await api<Record<string, any>>(`/api/discovery/jobs/${trackedJobId}`);
        if (!active) return;
        setPolledJob((previous) => previous && isTerminalDiscoveryJob(previous.status) && !isTerminalDiscoveryJob(current.status) ? previous : current);
        if (isTerminalDiscoveryJob(current.status)) { void load(); return; }
        timer = window.setTimeout(() => void poll(), 1_000);
      } catch (cause) {
        if (!active) return;
        setError(cause instanceof Error ? cause.message : "Unable to refresh discovery job status.");
        timer = window.setTimeout(() => void poll(), 1_000);
      }
    };
    void poll();
    return () => { active = false; if (timer !== undefined) window.clearTimeout(timer); };
  }, [trackedJobId, load]);
  const testSource = async () => { setTesting(true); try { const value = await post<any>("/api/discovery/source/test"); setData((current) => ({ ...(current || {}), source: value })); setError(null); } catch (cause) { setError(cause instanceof Error ? cause.message : "Unable to test source access."); } finally { setTesting(false); } };
  const start = async () => { try { const created = await post<any>("/api/discovery/jobs", { preset: selectedPreset, ...(limitOverride ? { candidate_limit: Number(limitOverride) } : {}), ...(activityOverride ? { min_activity: Number(activityOverride) } : {}), ...(windowOverride ? { window_hours: Number(windowOverride) } : {}), ...(ageOverride ? { max_activity_age: ageOverride } : {}) }); setTrackedJobId(created.job_id); setPolledJob(created); setError(null); } catch (cause) { setError(cause instanceof Error ? cause.message : "Unable to start candidate discovery."); } };
  const cancel = async () => { if (!job?.job_id) return; try { setPolledJob(await post(`/api/discovery/jobs/${job.job_id}/cancel`)); } catch (cause) { setError(cause instanceof Error ? cause.message : "Unable to request cancellation."); } };
  const result = job?.result || {};
  return <div className="page-grid">
    {error && <section className="panel span-12"><p className="empty-note">{error}</p></section>}
    <section className="panel span-6"><PanelTitle title="Candidate Source" subtitle="Official public HyperCore node data only" /><MetricList values={[["Source", source.source || "Official HyperCore node data"], ["Connection", source.connection_state || "SETUP REQUIRED"], ["AWS credentials", source.aws_credentials_detected ? "configured" : "missing"], ["AWS profile", source.aws_profile || "default provider chain"], ["Requester Pays", source.requester_pays_access || "UNTESTED"], ["Latest resolved hour", source.newest_available_data || "Not resolved yet"], ["Local cache", `${source.cache?.object_count || 0} objects — ${bytes(source.cache?.size_bytes)}`], ["Candidate universe", data?.candidate_universe_count ?? 0], ["Last successful discovery", data?.last_successful_discovery?.finished_at || "—"]]} /><p className="muted">{source.message || "No credentials are stored by Trader."}</p>{!sourceReady && <button className="button minor" onClick={() => void testSource()} disabled={testing}>{testing ? "Testing…" : "Test Source Access"}</button>}</section>
    <section className="panel span-6"><PanelTitle title="Research workflow" /><MetricList values={[["Step 1", "Discover traders"], ["Step 2", "Analyze traders"], ["Step 3", "Build Shadow cohort"], ["Step 4", "Select Active PAPER cohort"], ["Step 5", "Observe PAPER performance"]]} /><p className="muted">Discovery acquires bounded recent public data, invokes frozen Phase A, and never activates, shadows, watches, or paper-copies traders.</p></section>
    <section className="panel span-12"><PanelTitle title="Start Candidate Discovery" subtitle="Choose a deterministic count of completed UTC source hours; Phase B is never started automatically." /><div className="funnel">{(["quick", "standard", "deep"] as const).map((preset) => <button key={preset} className={selectedPreset === preset ? "selected" : ""} aria-pressed={selectedPreset === preset} onClick={() => setSelectedPreset(preset)}><strong>{preset.toUpperCase()} SCAN</strong><span>{presets[preset]?.hourly_objects || presets[preset]?.window_hours} completed hours — {Number(presets[preset]?.candidate_limit || 0).toLocaleString()} candidates — min {presets[preset]?.min_activity} events</span></button>)}</div><details><summary>Advanced scan overrides</summary><div className="toolbar"><input aria-label="Candidate limit override" type="number" min="1" max="5000" placeholder={`Candidate limit (${presets[selectedPreset]?.candidate_limit || 2500})`} value={limitOverride} onChange={(event) => setLimitOverride(event.target.value)} /><input aria-label="Minimum activity override" type="number" min="1" max="100" placeholder={`Minimum activity (${presets[selectedPreset]?.min_activity || 2})`} value={activityOverride} onChange={(event) => setActivityOverride(event.target.value)} /><input aria-label="Source window hours override" type="number" min="1" max="24" placeholder={`Source hours (${presets[selectedPreset]?.window_hours || 6})`} value={windowOverride} onChange={(event) => setWindowOverride(event.target.value)} /><input aria-label="Maximum activity age override" placeholder={`Max activity age (${presets[selectedPreset]?.max_activity_age || "30d"})`} value={ageOverride} onChange={(event) => setAgeOverride(event.target.value)} /></div></details><div className="toolbar"><span className="muted">Recency: {ageOverride || presets[selectedPreset]?.max_activity_age || "30d"}</span><button className="button positive" disabled={running || !sourceReady} onClick={() => confirmation({ title: "Start a PAPER research candidate scan?", body: "This will acquire recent public HyperCore data and run the frozen Phase A discovery pipeline. It will not place trades, activate traders, run Phase B automatically, or change existing operator states.", confirm: "Start Candidate Discovery", action: start })}>Start Candidate Discovery</button></div></section>
    {job && <section className="panel span-12"><PanelTitle title={`${String(job.configuration?.preset || selectedPreset).toUpperCase()} SCAN — ${String(job.status || "queued").toUpperCase()}`} subtitle={job.message || "Waiting for status."} /><MetricList values={[["Stage", job.stage || "queued"], ["Source hours", result.hourly_objects ?? result.source_plan?.objects_planned ?? job.configuration?.source_hour_count ?? "—"], ["Cached", result.source_plan ? `${result.source_plan.objects_cached} objects — ${bytes(result.source_plan.bytes_cached)}` : "—"], ["Download plan", result.source_plan ? `${result.source_plan.objects_planned - result.source_plan.objects_cached} objects — ${bytes(result.source_plan.bytes_to_download)}` : "—"], ["Acquisition", job.progress_total ? `${job.progress_current || 0} / ${job.progress_total} source objects` : "Indeterminate"], ["Source interval", result.source_first_hour ? `${result.source_first_hour} to ${result.source_last_hour}` : "—"], ["Observed wallets", result.wallets_observed === undefined ? "—" : Number(result.wallets_observed).toLocaleString()], ["Currently eligible", result.eligible_wallets === undefined ? "—" : Number(result.eligible_wallets).toLocaleString()], ["Candidates registered", result.registered_candidates === undefined ? "—" : Number(result.registered_candidates).toLocaleString()], ["New candidates", result.new_candidates === undefined ? "—" : Number(result.new_candidates).toLocaleString()], ["Existing refreshed", result.existing_refreshed === undefined ? "—" : Number(result.existing_refreshed).toLocaleString()], ["Filtered", result.filtered === undefined ? "—" : Number(result.filtered).toLocaleString()], ["Deferred by limit", result.deferred_by_limit === undefined ? "—" : Number(result.deferred_by_limit).toLocaleString()], ["Invalid", result.invalid === undefined ? "—" : Number(result.invalid).toLocaleString()], ["Candidate limit", job.configuration?.candidate_limit ?? "—"]]} />{running && <button className="button warning" onClick={() => void cancel()}>Cancel Discovery</button>}{["completed", "completed_with_warnings"].includes(job.status) && <div className="toolbar"><button className="button positive" onClick={() => { void refresh(); navigate("Candidates"); }}>Open Candidates</button><span className="muted">Analyze Candidates is intentionally manual; use the existing Phase B workflow after reviewing the discovery universe.</span></div>}</section>}
    {job?.status === "failed" && <section className="panel span-12 discovery-failure"><PanelTitle title="Discovery failed" subtitle="The persisted backend error is shown below; no cancellation remains pending." /><MetricList values={[["Stage", job.stage || "discovery"], ["Error", job.error?.message || job.message || "Unknown discovery error."], ["Downloaded", job.progress_total ? `${job.progress_current || 0} / ${job.progress_total}` : "—"], ["Download size", result.source_plan ? bytes(result.source_plan.bytes_total) : "—"]]} /><button className="button warning" onClick={() => confirmation({ title: "Retry candidate discovery?", body: "This retries the same bounded public-data workflow. Valid cached source objects are reused when they remain in the selected plan.", confirm: "Retry Discovery", action: start })}>Retry Discovery</button></section>}
  </div>;
}

function CandidatesPage({ notify, confirmation, refresh, navigate }: { notify: (toast: Toast) => void; confirmation: (value: Confirmation) => void; refresh: () => Promise<void>; navigate: (page: Page) => void }) {
  const [response, setResponse] = useState<CandidatesResponse | null>(null);
  const [search, setSearch] = useState(""); const [status, setStatus] = useState(""); const [lifecycle, setLifecycle] = useState("");
  const [sort, setSort] = useState("score"); const [direction, setDirection] = useState("desc"); const [compact, setCompact] = useState(false);
  const [page, setPage] = useState(1); const pageSize = 50; const [loadError, setLoadError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string[]>([]); const [detail, setDetail] = useState<Record<string, any> | null>(null);
  const load = useCallback(async () => {
    try {
      const params = new URLSearchParams({ page: String(page), page_size: String(pageSize), search, status, lifecycle, sort, direction });
      setResponse(await api<CandidatesResponse>(`/api/candidates?${params}`)); setLoadError(null);
    } catch (error) { const message = error instanceof Error ? error.message : "Could not load candidates."; setLoadError(message); notify({ tone: "error", message }); }
  }, [search, status, lifecycle, sort, direction, page, notify]);
  useEffect(() => { const timer = window.setTimeout(() => void load(), 180); return () => window.clearTimeout(timer); }, [load]);
  const order = (key: string) => { setPage(1); if (key === sort) setDirection(direction === "desc" ? "asc" : "desc"); else { setSort(key); setDirection("desc"); } };
  const act = async (wallet: string, state: string) => { try { const result = await post<any>(`/api/candidates/${wallet}/operator-state`, { state }); notify(result.cohort_over_recommended_size ? { tone: "warning", message: `Active PAPER cohort is ${result.active_count_after}; recommended maximum is ${result.recommended_max}.` } : { tone: "success", message: `Operator state updated to ${state}.` }); await load(); await refresh(); } catch (error) { notify({ tone: "error", message: error instanceof Error ? error.message : "Failed to update wallet. No state change was made." }); } };
  const requestState = (wallet: string, state: string) => confirmation({ title: `${state === "active" ? "Activate" : state === "rejected" ? "Reject" : "Change"} PAPER trader state?`, body: `Set ${walletLabel(wallet)} to ${state}. This changes paper-copy eligibility only; it never submits a live trade.`, confirm: `Confirm ${state}`, action: async () => { await act(wallet, state); } });
  const showDetail = async (wallet: string) => { try { setDetail(await api(`/api/candidates/${wallet}`)); } catch (error) { notify({ tone: "error", message: error instanceof Error ? error.message : "Could not open research detail." }); } };
  const toggle = (wallet: string) => setSelected((items) => items.includes(wallet) ? items.filter((item) => item !== wallet) : [...items, wallet]);
  return <div className="table-layout"><section className="panel table-panel">
    <div className="toolbar"><div><input aria-label="Search wallet" placeholder="Search wallet" value={search} onChange={(e) => { setSearch(e.target.value); setPage(1); }} /><select aria-label="Operator state" value={status} onChange={(e) => { setStatus(e.target.value); setPage(1); }}><option value="">All operator states</option>{["new", "approved", "shadow", "active", "muted", "rejected"].map((item) => <option key={item}>{item}</option>)}</select><select aria-label="Research state" value={lifecycle} onChange={(e) => { setLifecycle(e.target.value); setPage(1); }}><option value="">All research states</option>{["new", "prefilter_rejected", "backfill_pending", "analyzed", "qualified", "quarantined"].map((item) => <option key={item}>{item}</option>)}</select></div><div role="group" aria-label="Table density"><button className={compact ? "button minor" : "button minor selected"} aria-pressed={!compact} onClick={() => setCompact(false)}>Comfortable density</button><button className={compact ? "button minor selected" : "button minor"} aria-pressed={compact} onClick={() => setCompact(true)}>Compact density</button>{selected.length > 0 && <button className="button minor" onClick={() => confirmation({ title: "Add selected wallets to Shadow?", body: `${selected.length} selected wallets will become shadow paper candidates.`, confirm: "Add to Shadow", action: async () => { await Promise.all(selected.map((wallet) => act(wallet, "shadow"))); setSelected([]); } })}>Add {selected.length} to Shadow</button>}</div></div>
    <div className="table-scroll"><table className={compact ? "dense" : ""}><thead><tr><th /><SortHead label="Status" /><SortHead label="Wallet" active={sort === "wallet"} onClick={() => order("wallet")} /><SortHead label="Score" active={sort === "score"} onClick={() => order("score")} /><SortHead label="Last active" active={sort === "last_active"} onClick={() => order("last_active")} /><SortHead label="Campaigns" active={sort === "campaigns"} onClick={() => order("campaigns")} /><SortHead label="Target P&L" active={sort === "target_pnl"} onClick={() => order("target_pnl")} /><SortHead label="Follower P&L" active={sort === "follower_pnl"} onClick={() => order("follower_pnl")} /><SortHead label="Win rate" active={sort === "win_rate"} onClick={() => order("win_rate")} /><SortHead label="PF" active={sort === "profit_factor"} onClick={() => order("profit_factor")} /><SortHead label="Target DD" active={sort === "target_drawdown"} onClick={() => order("target_drawdown")} /><SortHead label="Follower DD" active={sort === "follower_drawdown"} onClick={() => order("follower_drawdown")} /><th>Copyability</th><th>Coverage</th><th>Sources</th></tr></thead>
      <tbody>{response?.items.map((item) => <tr key={item.wallet} className="click-row" onClick={() => void showDetail(item.wallet)}><td onClick={(e) => e.stopPropagation()}><input aria-label={`Select ${item.wallet}`} type="checkbox" checked={selected.includes(item.wallet)} onChange={() => toggle(item.wallet)} /></td><td><DualStatus candidate={item} /></td><td className="mono" title={item.wallet}>{walletLabel(item.wallet)}{item.stale_analysis && <span className="badge warning">STALE</span>}</td><td>{number(item.score)}</td><td>{timeLabel(item.last_active)}</td><td>{item.campaigns ?? "—"}</td><td className={Number(item.target_net_pnl) >= 0 ? "positive-text" : "negative-text"}>{money(item.target_net_pnl)}</td><td className={Number(item.follower_net_pnl) >= 0 ? "positive-text" : "negative-text"}>{money(item.follower_net_pnl)}</td><td>{percent(item.win_rate)}</td><td>{number(item.profit_factor, 2)}</td><td>{percent(item.target_max_drawdown)}</td><td>{percent(item.follower_max_drawdown)}</td><td>{item.copyability ?? "—"}</td><td><span className="badge neutral">{item.coverage || "UNPROVEN"}</span></td><td>{item.source_count ?? 0}</td></tr>)}{!response?.items.length && <tr><td colSpan={15} className="empty">{response?.total === 0 ? <><p>No candidate universe exists yet.</p><p>Run Candidate Discovery to scan recent public HyperCore trader activity and populate this research table.</p><button className="button positive" onClick={() => navigate("Discovery")}>Open Discovery</button></> : "No candidates match these filters."}</td></tr>}</tbody></table></div>
    <div className="table-footer"><span>{response?.total ?? 0} candidates — server-side query</span><span>Sorted {sort} {direction}</span></div>
    {loadError && <p className="empty-note">Unable to load candidates: {loadError}</p>}<div className="table-footer"><span>Page {response?.page ?? page} of {response?.pages ?? "—"}</span><span><button className="button minor" disabled={page <= 1} onClick={() => setPage((value) => Math.max(1, value - 1))}>Previous</button><button className="button minor" disabled={!response || page >= response.pages} onClick={() => setPage((value) => value + 1)}>Next</button></span></div>
  </section>{detail && <CandidateDossier detail={detail} close={() => setDetail(null)} action={requestState} />}</div>;
}

function ShadowPage({ notify, confirmation, refresh }: { notify: (toast: Toast) => void; confirmation: (value: Confirmation) => void; refresh: () => Promise<void> }) {
  const [items, setItems] = useState<any[]>([]);
  const load = useCallback(async () => { try { setItems((await api<{ items: any[] }>("/api/shadow-finalists")).items); } catch (error) { notify({ tone: "error", message: error instanceof Error ? error.message : "Unable to load Phase B finalists." }); } }, [notify]);
  useEffect(() => { void load(); }, [load]);
  const action = async (wallet: string, state: string) => { try { const result = await post<any>(`/api/candidates/${wallet}/operator-state`, { state }); notify(result.cohort_over_recommended_size ? { tone: "warning", message: `Active PAPER cohort is ${result.active_count_after}; recommended maximum is ${result.recommended_max}.` } : { tone: "success", message: `${walletLabel(wallet)} is now ${state}.` }); await load(); await refresh(); } catch (error) { notify({ tone: "error", message: error instanceof Error ? error.message : "Failed to update wallet. No state change was made." }); } };
  const requestState = (wallet: string, state: string) => confirmation({ title: `${state === "active" ? "Activate" : "Change"} PAPER trader state?`, body: `Set ${walletLabel(wallet)} to ${state}. This affects simulated-copy eligibility only.`, confirm: `Confirm ${state}`, action: async () => { await action(wallet, state); } });
  return <section className="panel table-panel"><PanelTitle title="Canonical Phase B shadow finalists" subtitle="Ranked by persisted Phase B qualification and diversification evidence — React does not recompute ranking." /><div className="table-scroll"><table><thead><tr><th>Rank</th><th>Wallet</th><th>Score</th><th>Primary strength</th><th>Principal risk</th><th>Target P&L</th><th>Follower P&L</th><th>Copyability</th><th>Diversification</th><th>Operator state</th><th>Actions</th></tr></thead><tbody>{items.map((item) => <tr key={item.wallet}><td>{item.rank}</td><td className="mono">{walletLabel(item.wallet)}</td><td>{number(item.score)}</td><td>{Object.keys(item.target || {})[0] || "Research evidence"}</td><td>{(item.principal_risks || ["—"])[0]}</td><td>{money(item.target?.net_pnl)}</td><td>{money(item.follower?.net_pnl)}</td><td>{item.copyability?.score ?? item.copyability?.status ?? "—"}</td><td>{item.diversification?.reason || item.diversification?.correlation_status || "—"}</td><td><span className="badge state">{item.operator_state}</span></td><td className="action-cell"><button onClick={() => requestState(item.wallet, "shadow")}>Add Shadow</button><button onClick={() => requestState(item.wallet, "active")}>Activate PAPER</button><button onClick={() => requestState(item.wallet, "muted")}>Mute</button></td></tr>)}{!items.length && <tr><td colSpan={11} className="empty">No current Phase B shadow finalists. Run Phase B analysis first.</td></tr>}</tbody></table></div></section>;
}

function ActivePage({ notify, confirmation, refresh }: { notify: (toast: Toast) => void; confirmation: (value: Confirmation) => void; refresh: () => Promise<void> }) {
  const [cohort, setCohort] = useState<Record<string, any> | null>(null);
  const load = useCallback(async () => { try { setCohort(await api("/api/active-cohort")); } catch (error) { notify({ tone: "error", message: error instanceof Error ? error.message : "Could not load active PAPER cohort." }); } }, [notify]);
  useEffect(() => { void load(); }, [load]);
  const remove = (wallet: string) => confirmation({ title: "Remove active PAPER trader?", body: "This removes the wallet from the active PAPER cohort. Existing PAPER positions retain normal exit handling and trader history remains intact.", confirm: "Remove From Active Cohort", action: async () => { try { await post(`/api/candidates/${wallet}/operator-state`, { state: "shadow" }); notify({ tone: "success", message: "Wallet removed from active PAPER cohort." }); await load(); await refresh(); } catch (error) { notify({ tone: "error", message: error instanceof Error ? error.message : "Failed to remove active wallet. No state change was made." }); } } });
  return <div className="page-grid"><section className="panel span-12"><PanelTitle title="Manual active PAPER cohort" subtitle={`Target size 5–7 — ${cohort?.count ?? 0} active — activation is manual only`} /><div className="table-scroll"><table><thead><tr><th>Wallet</th><th>Score</th><th>Allocation state</th><th>Open P&L</th><th>Total P&L</th><th>DD</th><th>Research state</th><th>Status</th><th /></tr></thead><tbody>{(cohort?.members || []).map((item: any) => <tr key={item.wallet}><td className="mono">{walletLabel(item.wallet)}</td><td>{number(item.score)}</td><td>{item.allocation_policy}</td><td>{money(item.open_pnl)}</td><td>{money(item.total_pnl)}</td><td>{percent(item.drawdown)}</td><td><span className="badge neutral">{item.research_state || "—"}</span></td><td><span className="badge state">ACTIVE — PAPER</span></td><td><button className="link-button" onClick={() => remove(item.wallet)}>Remove</button></td></tr>)}{!cohort?.members?.length && <tr><td className="empty" colSpan={9}>No active PAPER traders. Activate a qualified/shadow finalist from the Shadow page.</td></tr>}</tbody></table></div></section><section className="panel span-6"><PanelTitle title="Cohort guardrails" /><MetricList values={[["Paper only", "Yes"], ["Automatic promotion", "Disabled"], ["Entry eligibility", "Active state + global control"], ["Exit handling", "Continues while entries paused/muted"]]} /></section><section className="panel span-6"><PanelTitle title="Portfolio overlap" /><p className="muted">Current trader/symbol overlap is shown through the portfolio's attribution and positions views. The canonical Phase B finalist selection includes persisted diversification evidence before this manual activation step.</p></section></div>;
}

function PortfolioPage({ portfolio }: { portfolio: Portfolio | null }) {
  return <div className="page-grid"><section className="stat-grid span-12">{[["Paper equity", money(portfolio?.equity)], ["Free cash", money(portfolio?.cash)], ["Committed", money(portfolio?.committed_capital)], ["Open P&L", money(portfolio?.open_pnl)], ["Realized today", money(portfolio?.realized_pnl_today)], ["Realized total", money(portfolio?.realized_pnl_total)], ["Fees", money(portfolio?.fees)], ["Max DD", percent(portfolio?.max_drawdown)]].map(([label, value]) => <article className="panel stat" key={String(label)}><span>{label}</span><strong>{value}</strong></article>)}</section><section className="panel span-8"><PanelTitle title="Paper equity curve" subtitle="Persisted portfolio snapshots" /><MiniChart points={(portfolio?.equity_curve || []).map((point: any) => Number(point.equity || 0))} color="#46c995" /></section><section className="panel span-4"><PanelTitle title="Drawdown curve" /><MiniChart points={(portfolio?.drawdown_curve || []).map((point: any) => Number(point.value || 0))} color="#e7a950" /></section><Attribution title="P&L by trader" rows={portfolio?.pnl_by_trader || []} keys={["target_wallet", "open_pnl", "realized_pnl", "total_pnl", "fees", "capital_usage"]} /><Attribution title="P&L by symbol" rows={portfolio?.pnl_by_symbol || []} keys={["symbol", "open_pnl", "realized_pnl", "total_pnl", "exposure", "position_count"]} /><Attribution title="P&L by sizing bucket" rows={portfolio?.pnl_by_bucket || []} keys={["bucket", "open_pnl", "realized_pnl", "total_pnl", "capital_usage", "position_count"]} /></div>;
}

function PositionsPage({ livePositions }: { livePositions: any[] | null }) {
  const [data, setData] = useState<any[]>([]); const [wallet, setWallet] = useState(""); const [symbol, setSymbol] = useState(""); const [direction, setDirection] = useState(""); const [loadError, setLoadError] = useState<string | null>(null);
  const load = useCallback(async () => { try { const params = new URLSearchParams({ wallet, symbol, direction }); setData((await api<{ items: any[] }>(`/api/positions?${params}`)).items); setLoadError(null); } catch (error) { setLoadError(error instanceof Error ? error.message : "Could not load positions."); } }, [wallet, symbol, direction]);
  useEffect(() => { void load(); }, [load]);
  const shown = (livePositions ?? data).filter((item) => (!wallet || item.target_wallet?.includes(wallet.toLowerCase())) && (!symbol || item.symbol?.toUpperCase() === symbol.toUpperCase()) && (!direction || item.direction === direction));
  return <section className="panel table-panel"><div className="toolbar"><div><input placeholder="Filter wallet" value={wallet} onChange={(e) => setWallet(e.target.value)} /><input placeholder="Symbol" value={symbol} onChange={(e) => setSymbol(e.target.value)} /><select value={direction} onChange={(e) => setDirection(e.target.value)}><option value="">All directions</option><option>long</option><option>short</option></select></div><span className="paper-label">PAPER POSITIONS ONLY</span></div>{loadError && <p className="empty-note">Unable to load positions: {loadError}</p>}<div className="table-scroll"><table><thead><tr><th>PAPER</th><th>Target wallet</th><th>Symbol</th><th>Direction</th><th>Quantity</th><th>Entry</th><th>Mark</th><th>Allocated</th><th>Remaining</th><th>Bucket</th><th>Unrealized P&L</th><th>Realized P&L</th><th>Fees</th><th>Opened</th><th>Mark freshness</th></tr></thead><tbody>{shown.map((item) => <tr key={item.sleeve_id}><td><span className="badge paper">PAPER</span></td><td className="mono">{walletLabel(item.target_wallet)}</td><td>{item.symbol}</td><td>{item.direction}</td><td>{number(item.quantity, 4)}</td><td>{number(item.entry_price, 2)}</td><td>{number(item.current_mark, 2)}</td><td>{money(item.allocated_capital)}</td><td>{money(item.remaining_capital)}</td><td>{item.allocation_bucket}</td><td className={Number(item.unrealized_pnl) >= 0 ? "positive-text" : "negative-text"}>{money(item.unrealized_pnl)}</td><td>{money(item.realized_pnl)}</td><td>{money(item.fees)}</td><td>{timeLabel(item.opened_at)}</td><td><span className={`badge ${item.mark_fresh ? "good" : "warning"}`}>{item.mark_fresh ? "FRESH" : `STALE ${Math.round(item.mark_age_ms)}ms`}</span></td></tr>)}{!shown.length && <tr><td colSpan={15} className="empty">No open PAPER positions match these filters.</td></tr>}</tbody></table></div></section>;
}

function ActivityPage({ liveItems }: { liveItems: any[] | null }) {
  const [items, setItems] = useState<any[]>([]); const [wallet, setWallet] = useState(""); const [loadError, setLoadError] = useState<string | null>(null);
  const load = useCallback(async () => { try { setItems((await api<{ items: any[] }>(`/api/activity?${new URLSearchParams({ wallet })}`)).items); setLoadError(null); } catch (error) { setLoadError(error instanceof Error ? error.message : "Could not load activity."); } }, [wallet]);
  useEffect(() => { void load(); }, [load]);
  const shown = (liveItems ?? items).filter((item) => !wallet || item.wallet?.includes(wallet.toLowerCase()));
  return <section className="panel activity-panel"><div className="toolbar"><PanelTitle title="Paper system activity" subtitle="Operator actions, simulated execution, and notable operational state." /><input placeholder="Filter wallet" value={wallet} onChange={(e) => setWallet(e.target.value)} /></div>{loadError && <p className="empty-note">Unable to load activity: {loadError}</p>}<ActivityList items={shown} full /></section>;
}

function SystemPage({ control, watcherHealth }: { control: ControlState | null; watcherHealth: Record<string, any> | null }) {
  const [data, setData] = useState<any>(null); const [loadError, setLoadError] = useState<string | null>(null);
  useEffect(() => { void api("/api/system").then((value) => { setData(value); setLoadError(null); }).catch((error) => setLoadError(error instanceof Error ? error.message : "Could not load system status.")); }, []);
  const health = data?.health || {}; const risk = data?.risk || {}; const source = data?.source || {}; const apiRate = health.hyperliquid_api || {};
  const watcher = watcherHealth || health.watcher || {};
  const recovery = health.recovery?.wallets || [];
  return <div className="page-grid">{loadError && <section className="panel span-12"><p className="empty-note">Unable to load system status: {loadError}</p></section>}<section className="panel span-6"><PanelTitle title="System health" /><MetricList values={[["Mode", health.mode || "—"], ["Paper-only", health.paper_only ? "Yes" : "—"], ["Database", health.database?.connected ? "Connected" : "Degraded"], ["Watcher", watcher.state || "NOT_ATTACHED"], ["Supervisor", watcher.supervisor_state || watcher.state || "NOT_ATTACHED"], ["Desired targets", watcher.desired_target_count ?? 0], ["Subscribed targets", watcher.subscribed_target_count ?? 0], ["Membership", watcher.membership_in_sync ? "IN SYNC" : "OUT OF SYNC"], ["Active entry targets", watcher.active_entry_target_count ?? 0], ["Open sleeve wallets", watcher.open_sleeve_wallet_count ?? 0], ["Recovery gaps", recovery.filter((item: any) => item.state !== "CONTINUOUS").length], ["Market data", health.market_data?.fresh ? "Fresh" : "Stale / unavailable"], ["Kill switch", health.kill_switch?.active ? "ACTIVE" : "Off"]]} /></section><section className="panel span-6"><PanelTitle title="Paper control state" /><MetricList values={[["State", control?.state || "—"], ["New OPEN entries", control?.entries_allowed ? "Allowed" : "Paused"], ["Existing exits", "Enabled"], ["WebSocket", health.websocket?.available ? "Available" : "Unavailable"], ["Hyperliquid API", apiRate.state || "READY"], ["REST budget", `${Number(apiRate.estimated_weight_last_minute || 0).toLocaleString()} / ${Number(apiRate.documented_limit || 1200).toLocaleString()} weighted units`], ["429 retry signals", apiRate.retry_count ?? 0], ["HyperCore source", source.connection_state || "SETUP REQUIRED"], ["Cache size", `${Number(source.cache?.size_bytes || 0).toLocaleString()} bytes`], ["Last discovery", health.last_discovery_run?.started_at || "—"], ["Last Phase B", health.last_phase_b_run?.started_at || "—"]]} /></section><section className="panel span-12"><PanelTitle title="Risk controls — current / limit" /><div className="risk-grid">{(risk.limits || []).map((item: any) => <div key={item.label}><div><span>{item.label}</span><strong>{percent(item.current)} / {percent(item.limit)}</strong></div><div className="progress"><i style={{ width: `${Math.min(100, Number(item.current) / Math.max(Number(item.limit), 0.00001) * 100)}%` }} /></div></div>)}</div><p className="muted">These values reflect the existing paper risk gates. This GUI cannot override risk controls or bypass paper-mode validation.</p></section></div>;
}

function LaneIIIPaperPage({ notify, confirmation }: { notify: (toast: Toast) => void; confirmation: (value: Confirmation) => void }) {
  const [status, setStatus] = useState<any>(null); const [error, setError] = useState<string | null>(null); const [busy, setBusy] = useState(false);
  const load = useCallback(async () => {
    try { setStatus(await api<any>("/api/lane-iii/paper")); setError(null); }
    catch (failure) { setError(failure instanceof Error ? failure.message : "Lane III paper status is unavailable."); }
  }, []);
  useEffect(() => { void load(); const timer = window.setInterval(() => void load(), 2000); return () => window.clearInterval(timer); }, [load]);
  const act = async (path: string, label: string) => {
    if (busy) return; setBusy(true);
    try { const result = await post<any>(path); await load(); notify({ tone: result.armed === false || result.paused === false || result.resumed === false ? "warning" : "success", message: `${label}: ${String(result.state || "recorded")}` }); }
    catch (failure) { notify({ tone: "error", message: failure instanceof Error ? failure.message : `${label} failed.` }); }
    finally { setBusy(false); }
  };
  const policy = status?.policy || {}; const transport = status?.transport || {}; const lastDecision = status?.last_paper_decision || {}; const commissioning = status?.commissioning_lifecycle || {}; const ledger = status?.ledger || {};
  return <div className="page-grid">
    <section className="panel span-12 l3g-warning" role="alert"><strong>EXPERIMENTAL PAPER EXECUTION</strong><span>NOT SCIENTIFICALLY COMMISSIONED</span><span>SIM101 ONLY</span><span>LIVE CAPITAL DENIED</span></section>
    {error && <section className="panel span-12"><p className="empty-note">{error}</p></section>}
    <section className="panel span-6"><PanelTitle title="Lane III paper authority" subtitle="A separately labeled provisional policy; frozen Lane III science remains fail-closed." /><MetricList values={[["Market connection", status?.market_connection || "LucidFlex"], ["Market instrument", status?.market_instrument || "MNQ SEP26"], ["Paper account", status?.paper_account || "Sim101"], ["Mode", status?.display_mode || "EXPERIMENTAL PAPER"], ["Scientific Lane III", status?.scientific_lane_iii || "INCOMPLETE / BLOCKED ON SEQUENCING"], ["Paper execution", status?.paper_execution || "DISARMED"], ["Live capital", status?.live_capital || "DENIED"], ["Sequence authority", status?.sequence_authority || "LOCAL_CALLBACK_ORDER_ONLY"], ["Book completeness", status?.book_completeness || "UNVERIFIED"]]} /></section>
    <section className="panel span-6"><PanelTitle title="Execution truth" subtitle="Order acceptance is not treated as a fill; position truth comes from reconciliation callbacks." /><MetricList values={[["Runtime state", status?.state || "UNSTARTED"], ["Execution bridge", transport.state || "UNSTARTED"], ["Authenticated", transport.authenticated_client ? "Yes" : "No"], ["Reconciled", transport.reconciled ? "Yes" : "No"], ["Current position", `${status?.current_position || "FLAT"} ${status?.current_quantity || 0}`], ["Working owned orders", status?.working_owned_orders ?? 0], ["Protective stop", status?.protective_stop_state || "NONE"], ["Last reconciliation", status?.last_reconciliation?.timestamp || "—"], ["Lockout / fault", status?.lockout_or_fault_reason || "None"]]} /></section>
    <section className="panel span-6"><PanelTitle title="Session regime" subtitle="Asia Globex and New York RTH have isolated evidence; trade-date risk remains cumulative." /><MetricList values={[["Current session", status?.current_session || "OFF_SESSION"], ["Current session ID", status?.current_session_id || "—"], ["Trade date", status?.trade_date || "—"], ["Session state", status?.session_state || "—"], ["Entry window", status?.entry_window || "—"], ["Entry cutoff", status?.entry_cutoff || "—"], ["Hard flat", `${status?.hard_flat_deadline || "—"} America/New_York`], ["Session arm", status?.session_armed_state || "DISARMED"], ["Evidence warmup", status?.session_evidence_warmup ? "Ready" : "Warming"]]} /></section>
    <section className="panel span-6"><PanelTitle title="Paper risk" /><MetricList values={[["Session P&L", money(status?.session_pnl)], ["Asia session P&L", money(status?.asia_session_pnl)], ["New York session P&L", money(status?.new_york_session_pnl)], ["Combined trade-date P&L", money(status?.combined_trade_date_pnl ?? status?.daily_realized_pnl)], ["Loss allowance remaining", money(status?.combined_trade_date_loss_allowance_remaining)], ["Session entries", status?.session_entries ?? 0], ["Trade-date entries", status?.trade_date_entry_count ?? 0], ["Consecutive losses", status?.consecutive_losses ?? 0], ["Maximum position", "1 MNQ"], ["Daily loss limit", "$200"]]} /></section>
    <section className="panel span-6"><PanelTitle title="Latest provisional inference" /><MetricList values={[["Data quality", policy.quality || "UNUSABLE"], ["Classification", commissioning.classification || "STRATEGY_GENERATED_PAPER"], ["Last decision", lastDecision.decision || "—"], ["Hypothesis", lastDecision.hypothesis_kind || "—"], ["Relative support", lastDecision.relative_support || "—"], ["Reason", lastDecision.reason_code || "—"], ["Policy hash", status?.authority?.paper_policy_hash || "—"], ["Risk profile hash", status?.authority?.risk_profile_hash || "—"], ["Scientific eligibility", "False"]]} /></section>
    <section className="panel span-12"><PanelTitle title="Ledger health" subtitle="Cached chain state and lightweight filesystem metadata; full integrity scans remain explicit operations." /><MetricList values={[["Ledger path", ledger.path || "—"], ["Epoch ID", ledger.epoch_id || "UNSPECIFIED"], ["File size", optionalBytes(ledger.file_size)], ["Free space", optionalBytes(ledger.free_bytes)], ["Quick check", ledger.quick_check_state || "UNKNOWN"], ["Hash chain", ledger.chain_valid === true ? "VALID" : ledger.chain_valid === false ? "INVALID" : "UNKNOWN"], ["Broken identity", ledger.broken_identity || "None"], ["Highest sequence", ledger.highest_sequence ?? "—"], ["Last record", timeLabel(ledger.last_record_time)], ["WAL size", optionalBytes(ledger.wal_size)]]} /></section>
    <section className="panel span-12"><PanelTitle title="Paper controls" subtitle="Controls accept no account, instrument, quantity, action, or direction parameters." /><div className="l3g-controls"><button className="button positive" disabled={busy || status?.state !== "READY_DISARMED"} onClick={() => void act("/api/lane-iii/paper/arm", "Arm Paper")}>Arm Paper</button><button className="button positive" disabled={busy || status?.state !== "ARMED_FLAT"} onClick={() => confirmation({ title: "Submit explicit commissioning entry?", body: "This sends one sealed, non-strategy 1 MNQ Sim101 commissioning entry through the normal paper safety gates.", confirm: "Run Commissioning Entry", action: () => act("/api/lane-iii/paper/commission-entry", "Commissioning Entry") })}>Run Commissioning Entry</button><button className="button warning" disabled={busy || !commissioning.active} onClick={() => confirmation({ title: "Close explicit commissioning position?", body: "This sends the normal owned exit, then requires a fresh flat/order reconciliation before it disarms the paper runtime.", confirm: "Run Commissioning Exit", action: () => act("/api/lane-iii/paper/commission-exit", "Commissioning Exit") })}>Run Commissioning Exit</button><button className="button warning" disabled={busy} onClick={() => void act("/api/lane-iii/paper/pause", "Pause Entries")}>Pause Entries</button><button className="button positive" disabled={busy || status?.state !== "PAUSED"} onClick={() => void act("/api/lane-iii/paper/resume", "Resume Entries")}>Resume Entries</button><button className="button critical" disabled={busy} onClick={() => confirmation({ title: "Flatten exact Sim101 / MNQ SEP26 and disarm?", body: "This cancels Beelzebub-owned MNQ orders and initiates an exact-instrument Sim101 safety exit. Flat status is not assumed until NinjaTrader reconciliation confirms it.", confirm: "Flatten and Disarm", action: () => act("/api/lane-iii/paper/flatten-and-disarm", "Flatten and Disarm") })}>Flatten and Disarm</button></div></section>
  </div>;
}

function ShadowObservationStatus() {
  const [shadow, setShadow] = useState<any>(null); const [error, setError] = useState<string | null>(null); const [refreshing, setRefreshing] = useState(false);
  const load = useCallback(async () => {
    try { setShadow((await api<any>("/api/execution")).shadow || {}); setError(null); }
    catch (failure) { setError(failure instanceof Error ? failure.message : "Could not load shadow observation."); }
  }, []);
  useEffect(() => { void load(); }, [load]);
  const refresh = async () => {
    if (refreshing) return;
    setRefreshing(true);
    try { setShadow(await post<any>("/api/execution/shadow/refresh")); setError(null); }
    catch (failure) { setError(failure instanceof Error ? failure.message : "Could not refresh shadow observation."); }
    finally { setRefreshing(false); }
  };
  return <section className="panel shadow-observation-panel"><PanelTitle title="Real-venue shadow observation" subtitle="Public-account reads only. This cannot sign, submit, cancel, or enable live execution." />{error && <p className="empty-note">{error}</p>}{refreshing && <p className="muted" role="status">Refreshing read-only evidence. Existing evidence is pending replacement and is not newly verified.</p>}<MetricList values={[["Configured", shadow?.configured ? "Yes" : "No"], ["Venue", shadow?.venue || "—"], ["Account", shadow?.account_id || "—"], ["Observation", shadow?.state || "NOT_CONFIGURED"], ["Freshness", shadow?.freshness || "UNKNOWN"], ["Last observed", shadow?.latest_observation?.observed_at || "—"], ["Last received", shadow?.latest_observation?.received_at || "—"], ["Position comparison", shadow?.latest_observation?.comparison?.positions?.state || "—"], ["Open-order comparison", shadow?.latest_observation?.comparison?.open_orders?.state || "—"]]} />{shadow?.configured ? <button className="button minor" onClick={() => void refresh()} disabled={refreshing}>{refreshing ? "Refreshing read-only shadow observation" : "Refresh read-only shadow observation"}</button> : <p className="muted">Shadow observation is not configured. It is distinct from simulator and PAPER execution authority.</p>}</section>;
}

function CandidateDossier({ detail, close, action }: { detail: Record<string, any>; close: () => void; action: (wallet: string, state: string) => void }) {
  const identity = detail.identity || {}; const score = detail.score || {}; const wallet = identity.wallet;
  const recommendation = detail.finalist_recommendation || {};
  const selectedFinalist = recommendation.finalist_eligible === true
    && recommendation.selection_rank !== null && recommendation.selection_rank !== undefined;
  return <aside className="dossier"><header><div><span className="eyebrow">RESEARCH DOSSIER</span><h2 className="mono">{walletLabel(wallet)}</h2></div><button className="icon-button" onClick={close} aria-label="Close dossier">×</button></header><div className="dossier-actions"><button onClick={() => void action(wallet, "shadow")}>Add to Shadow</button>{selectedFinalist && <button onClick={() => void action(wallet, "active")}>Activate PAPER</button>}<button onClick={() => void action(wallet, "muted")}>Mute</button><button onClick={() => void action(wallet, "rejected")}>Reject</button></div>{!selectedFinalist && <p className="muted">Activation is available only for a current Phase B selected finalist.</p>}<section><h3>Identity & state</h3><MetricList values={[["Operator", identity.operator_state], ["Research", identity.research_state], ["Last activity", timeLabel(identity.last_activity)], ["Analysis", timeLabel(identity.analysis_timestamp)], ["Coverage", identity.coverage?.coverage_state || identity.coverage?.status || "UNPROVEN"]]} /></section><section><h3>Score {number(score.total)}</h3><p className="muted">Eligibility: {score.eligible ? "qualified" : "not qualified"}</p><ScoreBars values={score.components || {}} penalties={score.penalties || {}} /><ReasonList label="Phase B score reasons" values={score.reasons || []} /><ReasonList label="Phase B hard-gate failures" values={detail.phase_b_hard_gates || []} /><ReasonList label="Phase A prefilter reasons" values={detail.phase_a_prefilter_reasons || []} /></section><section><h3>Target performance</h3><Performance values={detail.target_performance || {}} /></section><section><h3>Follower performance</h3><Performance values={detail.follower_performance || {}} /></section><section><h3>Copyability & slippage</h3><pre>{JSON.stringify({ copyability: detail.copyability, slippage: detail.slippage }, null, 2)}</pre></section><section><h3>Latency</h3>{detail.latency?.status === "unavailable" ? <p className="empty-note">Historical latency evidence unavailable</p> : <pre>{JSON.stringify(detail.latency, null, 2)}</pre>}</section><section><h3>Walk-forward</h3><pre>{JSON.stringify(detail.walk_forward, null, 2)}</pre></section><section><h3>Immutable Phase B analysis window</h3><pre>{JSON.stringify(detail.analysis_window, null, 2)}</pre></section></aside>;
}

function ConfirmationDialog({ item, close }: { item: NonNullable<Confirmation>; close: () => void }) {
  const [busy, setBusy] = useState(false); const confirm = async () => { setBusy(true); await item.action(); setBusy(false); close(); };
  return <div className="modal-backdrop" role="presentation"><section className="modal" role="dialog" aria-modal="true" aria-labelledby="confirm-title"><span className="eyebrow">CONSEQUENTIAL PAPER ACTION</span><h2 id="confirm-title">{item.title}</h2><p>{item.body}</p><div><button className="button minor" onClick={close} disabled={busy}>Cancel</button><button className="button critical" onClick={() => void confirm()} disabled={busy}>{busy ? "Working…" : item.confirm}</button></div></section></div>;
}

function PanelTitle({ title, subtitle, action, onAction }: { title: string; subtitle?: string; action?: string; onAction?: () => void }) { return <div className="panel-title"><div><h2>{title}</h2>{subtitle && <p>{subtitle}</p>}</div>{action && <button className="link-button" onClick={onAction}>{action} →</button>}</div>; }
function MetricList({ values }: { values: [string, unknown][] }) { return <dl className="metric-list">{values.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{String(value ?? "—")}</dd></div>)}</dl>; }
function SortHead({ label, active, onClick }: { label: string; active?: boolean; onClick?: () => void }) { return <th><button className={active ? "sort active" : "sort"} aria-pressed={Boolean(active)} onClick={onClick}>{label}{active ? " ↓" : ""}</button></th>; }
function DualStatus({ candidate }: { candidate: Candidate }) { return <span className="status-stack"><span className="badge state">{candidate.operator_state}</span><span className="badge neutral">{candidate.research_state}</span></span>; }
function CandidateRows({ candidates }: { candidates: Candidate[] }) { return <div className="candidate-rows">{candidates.map((item) => <div key={item.wallet}><span className="mono">{walletLabel(item.wallet)}</span><DualStatus candidate={item} /><strong>{number(item.score)}</strong><span>{money(item.follower_net_pnl)}</span></div>)}{!candidates.length && <p className="empty-note">No persisted candidate summaries yet.</p>}</div>; }
function ActivityList({ items, full = false }: { items: any[]; full?: boolean }) { return <ol className={full ? "activity-list full" : "activity-list"}>{items.map((item) => <li key={item.event_id}><time>{new Date(item.occurred_at).toLocaleTimeString()}</time><span className={`event-dot ${item.severity || "info"}`} /><div><strong>{item.category}</strong><p>{item.message}</p>{item.wallet && <small className="mono">{walletLabel(item.wallet)} {item.symbol || ""}</small>}</div></li>)}{!items.length && <li className="empty-note">No recorded activity yet.</li>}</ol>; }
function ScoreBars({ values, penalties }: { values: Record<string, number>; penalties: Record<string, number> }) { return <div className="score-bars">{Object.entries(values).map(([key, value]) => <div key={key}><span>{key.replaceAll("_", " ")}</span><i><b style={{ width: `${Math.min(100, Math.max(0, Number(value)))}%` }} /></i><strong>{number(value)}</strong></div>)}{Object.entries(penalties).map(([key, value]) => <div className="penalty" key={key}><span>{key.replaceAll("_", " ")} penalty</span><i><b style={{ width: `${Math.min(100, Math.max(0, Number(value)))}%` }} /></i><strong>-{number(value)}</strong></div>)}</div>; }
function ReasonList({ label, values }: { label: string; values: string[] }) { return <div className="reason-list"><span>{label}</span>{values.length ? values.map((item) => <em key={item}>{item}</em>) : <small>None recorded</small>}</div>; }
function Performance({ values }: { values: Record<string, any> }) { const keys = ["net_pnl", "gross_pnl", "fees", "win_rate", "profit_factor", "expectancy", "campaign_count", "max_drawdown", "liquidation_count"]; return <MetricList values={keys.filter((key) => values[key] !== undefined).map((key) => [key.replaceAll("_", " "), key.includes("rate") || key.includes("drawdown") ? percent(values[key]) : key.includes("pnl") || key === "fees" || key === "expectancy" ? money(values[key]) : values[key]]) as [string, unknown][]} />; }
function MiniChart({ points, color }: { points: number[]; color: string }) { const path = useMemo(() => { if (points.length < 2) return ""; const min = Math.min(...points), max = Math.max(...points), range = max - min || 1; return points.map((value, index) => `${index ? "L" : "M"}${(index / (points.length - 1)) * 100} ${92 - ((value - min) / range) * 80}`).join(" "); }, [points]); return <div className="chart">{points.length > 1 ? <svg viewBox="0 0 100 100" preserveAspectRatio="none"><path d={path} fill="none" stroke={color} strokeWidth="1.5" vectorEffect="non-scaling-stroke" /></svg> : <p className="empty-note">Awaiting persisted portfolio snapshots.</p>}</div>; }
function Attribution({ title, rows, keys }: { title: string; rows: any[]; keys: string[] }) { return <section className="panel span-4"><PanelTitle title={title} /><div className="table-scroll small-table"><table><thead><tr>{keys.map((key) => <th key={key}>{key.replaceAll("_", " ")}</th>)}</tr></thead><tbody>{rows.map((row) => <tr key={String(row[keys[0]])}>{keys.map((key) => <td key={key} className={key.includes("pnl") && Number(row[key]) < 0 ? "negative-text" : ""}>{key.includes("pnl") || key === "fees" || key.includes("capital") || key === "exposure" ? money(row[key]) : row[key]}</td>)}</tr>)}{!rows.length && <tr><td colSpan={keys.length} className="empty">No paper attribution available.</td></tr>}</tbody></table></div></section>; }
