import { useCallback, useEffect, useState } from "react";
import { api, post } from "./api";

type LaneIIStatus = Record<string, any>;
type Toast = { tone: "success" | "warning" | "error"; message: string } | null;

const money = (value: unknown) => new Intl.NumberFormat("en-US", {
  style: "currency", currency: "USD", maximumFractionDigits: 2,
}).format(Number(value || 0));
const shortWallet = (wallet: string) => wallet.length > 14 ? `${wallet.slice(0, 8)}…${wallet.slice(-6)}` : wallet;
const timeLabel = (value?: string | null) => value ? new Date(value).toLocaleString() : "Not available";

export function LaneIISlimConsole({ onFullConsole }: { onFullConsole: () => void }) {
  const [status, setStatus] = useState<LaneIIStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [toast, setToast] = useState<Toast>(null);

  const load = useCallback(async () => {
    try {
      setStatus(await api<LaneIIStatus>("/api/lane-ii/slim-status"));
      setError(null);
    } catch (failure) {
      setStatus(null);
      setError(failure instanceof Error ? failure.message : "Lane II status is unavailable.");
    }
  }, []);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 5_000);
    return () => window.clearInterval(timer);
  }, [load]);

  const act = useCallback(async (name: string, path: string, success: string) => {
    if (busy) return;
    setBusy(name);
    try {
      await post(path);
      await load();
      setToast({ tone: "success", message: success });
    } catch (failure) {
      await load();
      setToast({ tone: "error", message: failure instanceof Error ? failure.message : `${name} failed.` });
    } finally {
      setBusy(null);
    }
  }, [busy, load]);

  const readiness = status?.readiness || {};
  const cohort = status?.cohort || {};
  const selected = cohort.selected || [];
  const watchlist = cohort.research_watchlist || [];
  const candidates = selected.length ? selected : watchlist;
  const portfolio = status?.portfolio || {};
  const policy = status?.paper_policy || {};
  const controls = status?.controls || {};
  const shortfall = cohort.shortfall || {};
  const paperStartBlocker = controls.start_paper_blocker || readiness.paper?.blocker;

  return <div className="lane-ii-slim">
    <header className="lane-ii-hero">
      <div>
        <div className="eyebrow">LANE II · PUBLIC OBSERVATION + LOCAL SIMULATION</div>
        <h1>Copy Trading</h1>
        <p className="lane-ii-account">PAPER — $100 simulated</p>
      </div>
      <button className="button minor" onClick={onFullConsole}>Full Console</button>
    </header>

    {error && <section className="lane-ii-alert error" role="alert"><strong>Connection lost</strong><span>{error}</span><small>No cached balance or readiness state is being shown.</small></section>}
    {!status && !error && <section className="lane-ii-alert"><strong>Loading Lane II…</strong><span>Reading the isolated paper ledger and public-observer state.</span></section>}

    {status && <>
      <section className="lane-ii-state">
        <div><span>Overall state</span><strong>{String(status.overall?.state || "UNKNOWN").replaceAll("_", " ")}</strong></div>
        <p>{status.overall?.next_action}</p>
      </section>

      <section className="lane-ii-readiness" aria-label="Independent readiness states">
        {[
          ["Public data", readiness.public_observation?.state, "Read-only; no signer"],
          ["Paper execution", readiness.paper?.state, "Local simulation only"],
          ["Testnet", readiness.testnet?.state, "Not commissioned"],
          ["Live trading", readiness.live?.state, "Eligibility unresolved · backend denied"],
        ].map(([label, value, detail]) => <article className="lane-ii-status-card" key={label}>
          <span>{label}</span><strong>{String(value || "UNKNOWN").replaceAll("_", " ")}</strong><small>{detail}</small>
        </article>)}
      </section>

      <section className="lane-ii-controls" aria-label="Lane II controls">
        <button className="button minor" disabled={Boolean(busy) || !controls.refresh_candidates_available} onClick={() => void act("Refresh Candidates", "/api/lane-ii/candidates/refresh", "Candidate evidence refresh completed.")}>{busy === "Refresh Candidates" ? "Refreshing…" : "Refresh Candidates"}</button>
        <button className="button positive" disabled={Boolean(busy) || !controls.observe_available} onClick={() => void act("Observe", "/api/lane-ii/observe", "Read-only public observation requested.")}>{busy === "Observe" ? "Starting…" : "Observe"}</button>
        <button className="button positive" title={!controls.start_paper_available ? paperStartBlocker : undefined} disabled={Boolean(busy) || !controls.start_paper_available} onClick={() => void act("Start Paper Copying", "/api/lane-ii/paper/start", "Lane II PAPER copying started after backend confirmation.")}>{busy === "Start Paper Copying" ? "Starting…" : "Start Paper Copying"}</button>
        <button className="button warning" disabled={Boolean(busy)} onClick={() => void act("Pause New Entries", "/api/controls/pause-entries", "New Lane II PAPER entries paused; exits remain available.")}>Pause New Entries</button>
        <button className="button critical outline" disabled={Boolean(busy) || !controls.close_paper_positions_available} onClick={() => void act("Close Paper Positions", "/api/controls/close-all-paper-positions", "Backend completed the PAPER close request.")}>Close Paper Positions</button>
        <button className="button critical" disabled title="Unavailable: U.S. venue eligibility is unresolved and no live adapter exists.">Live Trading Unavailable</button>
      </section>
      {!controls.start_paper_available && paperStartBlocker && <p className="lane-ii-control-blocker" role="status">{paperStartBlocker}</p>}

      <section className="lane-ii-funnel" aria-label="Lane II discovery funnel">
        {(status.funnel || []).map((item: any) => <div key={item.label}><strong>{Number(item.count || 0).toLocaleString()}</strong><span>{item.label}</span></div>)}
      </section>

      <section className="lane-ii-grid">
        <article className="lane-ii-panel lane-ii-portfolio">
          <div className="lane-ii-section-title"><div><span>SIMULATED FOLLOWER</span><h2>Paper account</h2></div><small>Connected capital: No</small></div>
          <div className="lane-ii-metrics">
            <div><span>Equity</span><strong>{money(portfolio.equity)}</strong></div>
            <div><span>Free capital</span><strong>{money(portfolio.cash)}</strong></div>
            <div><span>Exposure</span><strong>{money(portfolio.committed_capital)}</strong></div>
            <div><span>Net P&amp;L</span><strong>{money(Number(portfolio.open_pnl || 0) + Number(portfolio.realized_pnl_total || 0))}</strong></div>
            <div><span>Fees</span><strong>{money(portfolio.fees)}</strong></div>
            <div><span>Funding</span><strong>{policy.funding_model === "NOT_MODELED" ? "Not modeled" : money(portfolio.funding)}</strong></div>
            <div><span>Remaining risk budget</span><strong>{money(Math.max(0, Number(policy.maximum_total_committed || 0) - Number(portfolio.committed_capital || 0)))}</strong></div>
          </div>
        </article>

        <article className="lane-ii-panel lane-ii-setup">
          <div className="lane-ii-section-title"><div><span>CONNECTION BOUNDARY</span><h2>Setup</h2></div></div>
          <ul>
            <li><strong>Location:</strong> Colorado, United States</li>
            <li><strong>Future budget:</strong> Cash App BTC — not connected</li>
            <li><strong>Trading account:</strong> Unknown</li>
            <li><strong>API/agent signer:</strong> Not configured here</li>
            <li><strong>Live eligibility:</strong> Unresolved; no workaround or inference</li>
          </ul>
        </article>

        <article className="lane-ii-panel lane-ii-cohort">
          <div className="lane-ii-section-title"><div><span>{selected.length ? "FROZEN SELECTED COHORT" : "UNQUALIFIED RESEARCH WATCHLIST"}</span><h2>{selected.length ? `${selected.length} selected traders` : "Evidence shortfall"}</h2></div><small>Cutoff {timeLabel(cohort.data_cutoff)}</small></div>
          {!selected.length && shortfall.summary && <p className="lane-ii-empty">{shortfall.summary}</p>}
          {!candidates.length && <p className="lane-ii-empty">No fresh cohort evidence is available. Refresh Candidates performs public reads and the unchanged Phase B gates.</p>}
          <div className="lane-ii-candidate-list">{candidates.map((item: any) => <article className="lane-ii-candidate" key={item.wallet}>
            <div><strong title={item.wallet}>{shortWallet(item.wallet)}</strong><span>{selected.length ? "Qualified finalist" : "Research only"}</span></div>
            <dl>
              <div><dt>Score</dt><dd>{item.score ?? item.final_selection_score ?? "—"}</dd></div>
              <div><dt>Campaigns</dt><dd>{item.campaigns ?? item.target?.activity?.campaigns ?? "—"}</dd></div>
              <div><dt>Coverage</dt><dd>{String(item.coverage || item.data_quality?.coverage_state || "UNPROVEN")}</dd></div>
              <div><dt>Follower net</dt><dd>{item.follower_net_pnl === null || item.follower_net_pnl === undefined ? "Unknown" : money(item.follower_net_pnl)}</dd></div>
              <div><dt>Last active</dt><dd>{timeLabel(item.recent_activity_at || item.activity?.last_seen_at)}</dd></div>
            </dl>
            <small>{item.selection_reason || item.qualification_warning || "Not selected under current evidence gates."}</small>
          </article>)}</div>
        </article>

        <article className="lane-ii-panel">
          <div className="lane-ii-section-title"><div><span>VIRTUAL OWNERSHIP</span><h2>Copied positions</h2></div></div>
          {!(status.positions || []).length ? <p className="lane-ii-empty">No open PAPER sleeves. Initial snapshots never create retroactive entries.</p> :
            <div className="lane-ii-list">{status.positions.map((position: any) => <div key={position.sleeve_id}><strong>{position.symbol} · {position.direction}</strong><span>{shortWallet(position.target_wallet)} · {position.quantity} · {money(position.unrealized_pnl)}</span></div>)}</div>}
        </article>

        <article className="lane-ii-panel">
          <div className="lane-ii-section-title"><div><span>AUDIT FEED</span><h2>Recent actions</h2></div></div>
          {!(status.recent_actions || []).length ? <p className="lane-ii-empty">No Lane II actions recorded yet.</p> :
            <div className="lane-ii-list">{status.recent_actions.slice(0, 8).map((item: any) => <div key={item.event_id}><strong>{item.message}</strong><span>{timeLabel(item.occurred_at)}{item.wallet ? ` · ${shortWallet(item.wallet)}` : ""}</span></div>)}</div>}
        </article>
      </section>
    </>}

    {toast && <div className={`toast ${toast.tone}`} role="alert"><span className="toast-title">{toast.tone === "error" ? "Action failed" : "Updated"}</span><span className="toast-message">{toast.message}</span><button aria-label="Dismiss notification" onClick={() => setToast(null)}>×</button></div>}
  </div>;
}
