import type { PaperConsoleState } from "./paperConsole";

type Props = {
  paper: PaperConsoleState;
  onFullConsole: () => void;
};

const money = (value: unknown) => {
  const numeric = Number(value);
  return Number.isFinite(numeric)
    ? new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(numeric)
    : "—";
};

const pnlClass = (value: unknown) => {
  const numeric = Number(value);
  return !Number.isFinite(numeric) || numeric === 0 ? "neutral" : numeric > 0 ? "positive" : "negative";
};

export function SlimConsole({ paper, onFullConsole }: Props) {
  const status = paper.slimStatus;
  const light = status?.light || "RED";
  const active = status?.paper_active === true;
  const runtimeState = String(paper.status?.state || "");
  const runtimeMayBeActive = paper.status?.operational_paper_session?.active === true
    || ["STARTING", "PAPER_RUNNING", "ENTRY_PENDING", "OPEN_POSITION", "EXIT_PENDING", "PAUSED", "RECONCILING", "FAULTED", "LOCKED_OUT"].includes(runtimeState);
  // Keep the stop control available if the concise status cannot be refreshed
  // but the last canonical runtime snapshot could still represent authority.
  // Any non-green presentation is safety-ambiguous from the operator's point
  // of view. Keep the idempotent stop path visible rather than making them
  // infer whether a red or yellow transition still owns paper authority.
  const stopAvailable = active || runtimeMayBeActive || status === null || light !== "GREEN";
  const verification = status?.ledger_verification || {};
  const maintenance = paper.ninjaTraderMaintenance;
  const maintenanceButton = maintenance?.button || { label: "Checking NinjaTrader…", enabled: false, tone: "progress" };
  const maintenanceDisabled = maintenanceButton.enabled !== true || maintenance?.in_progress === true || paper.maintenanceBusy;
  const pnl = status?.pnl || { state: "MISSING" };
  const session = status?.session || {};
  const verificationRunning = verification.state === "IN_PROGRESS" || paper.verificationInFlight;
  const startEnabled = status?.can_start === true && !paper.busy;
  const pnlUnavailable = pnl.state !== "CURRENT";
  const resultLabel = verificationRunning
    ? "Verifying…"
    : verification.state === "PASS" ? `Verified${verification.completed_at ? ` · ${new Date(verification.completed_at).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}` : ""}`
    : verification.state === "FAIL" ? verification.message || "Verification failed"
    : verification.message || "Verification required";

  return <main className="slim-console" aria-label="BeezConsole Slim Mode">
    <header className="slim-header">
      <div className="slim-brand"><span className="slim-brand-mark" aria-hidden="true">B</span><span>BeezConsole <b>Slim</b></span></div>
      <button className="slim-mode-button" type="button" onClick={onFullConsole}>Full Console</button>
    </header>

    <section className="slim-card slim-readiness" aria-labelledby="slim-readiness-heading">
      <div className="slim-kicker">BEELZEBUB READINESS</div>
      <div className="slim-lights" role="img" aria-label={`Readiness: ${light}`}>
        {(["RED", "YELLOW", "GREEN"] as const).map((item) => <span className={`slim-light ${item.toLowerCase()} ${light === item ? "illuminated" : ""}`} key={item} aria-hidden="true" />)}
      </div>
      <h1 id="slim-readiness-heading">{status?.label || "NOT READY"}</h1>
      <p className="slim-message" role="status" aria-live="polite">{status?.message || "Waiting for current canonical paper runtime status."}</p>
      <p className="slim-session">Session: <strong>{session.session_kind || "OFF_SESSION"}</strong>{session.session_family ? ` / ${session.session_family}` : ""}{session.entry_window ? ` · ${session.entry_window}` : ""}</p>
    </section>

    <section className="slim-card slim-ninjatrader" aria-labelledby="slim-ninjatrader-heading">
      <div className="slim-kicker">NINJATRADER / MNQ OBSERVER</div>
      <h2 id="slim-ninjatrader-heading" className={maintenance?.readiness === "READY" ? "positive" : "neutral"}>
        {maintenance?.readiness === "READY" ? "READY" : maintenance?.stage || "CHECKING"}
      </h2>
      <button
        className={`slim-action ninja-maintenance ${maintenanceButton.tone || "primary"}`}
        type="button"
        disabled={maintenanceDisabled}
        onClick={() => void paper.startNinjaTraderMaintenance()}
      >
        {paper.maintenanceBusy ? "Starting maintenance…" : maintenanceButton.label}
      </button>
      <dl className="slim-maintenance-status">
        <div><dt>Process</dt><dd>{maintenance?.process?.state || "UNKNOWN"}</dd></div>
        <div><dt>AddOn / provenance</dt><dd>{maintenance ? `${maintenance.addon?.state || "UNKNOWN"} / ${maintenance.addon?.provenance || "UNVERIFIED"}` : "UNKNOWN"}</dd></div>
        <div><dt>Configured instrument</dt><dd>{maintenance?.configured_instrument || "UNRESOLVED"}</dd></div>
        <div><dt>Chart</dt><dd>{maintenance?.chart?.found ? `FOUND / ${maintenance.chart.instrument || "UNKNOWN"}` : maintenance?.chart?.state || "NOT FOUND"}</dd></div>
        <div><dt>Observer</dt><dd>{maintenance?.observer?.attached ? "ATTACHED" : "NOT ATTACHED"}</dd></div>
        <div><dt>Market data</dt><dd>{maintenance?.observer?.market_data_fresh ? "FRESH" : maintenance?.observer?.freshness_reason || "NOT FRESH"}</dd></div>
      </dl>
      {maintenance?.blockers?.length > 0 && <p className="slim-maintenance-blocker" role="alert">Blocked: {maintenance.blockers.join(", ")}</p>}
      {maintenance?.manual_action && <p className="slim-maintenance-manual" role="status">Manual step: {maintenance.manual_action}</p>}
    </section>

    <section className="slim-card slim-pnl" aria-labelledby="slim-pnl-heading">
      <div className="slim-kicker">PAPER / SESSION P&amp;L</div>
      <h2 id="slim-pnl-heading" className={pnlUnavailable ? "neutral" : pnlClass(pnl.total)}>{pnlUnavailable ? (pnl.state === "STALE" ? "STALE" : "—") : money(pnl.total)}</h2>
      <dl>
        <div><dt>Realized</dt><dd className={pnlUnavailable ? "neutral" : pnlClass(pnl.realized)}>{pnlUnavailable ? "—" : money(pnl.realized)}</dd></div>
        <div><dt>Unrealized</dt><dd className={pnlUnavailable ? "neutral" : pnlClass(pnl.unrealized)}>{pnlUnavailable ? "—" : money(pnl.unrealized)}</dd></div>
      </dl>
    </section>

    <section className="slim-actions" aria-label="Paper controls">
      <button className="slim-action verification" type="button" disabled={paper.busy || verificationRunning} onClick={() => void paper.startVerification()} aria-describedby="slim-verification-result">
        Ledger Verification
      </button>
      <p id="slim-verification-result" className={`slim-verification-result ${verification.state === "FAIL" ? "failed" : ""}`} role="status" aria-live="polite">{resultLabel}</p>
      {stopAvailable
        ? <button className="slim-action stop" type="button" disabled={paper.busy} onClick={() => void paper.stopAndDisarm()}>{paper.busy ? "STOPPING…" : "STOP TRADING"}</button>
        : <button className="slim-action start" type="button" disabled={!startEnabled} onClick={() => void paper.startPaperTrading()} aria-describedby="slim-readiness-heading">{paper.busy ? "Starting…" : "Start Paper Trading"}</button>}
    </section>
    {paper.error && <p className="slim-error" role="alert">Status unavailable — {paper.error}</p>}
    <p className="slim-footnote">Paper-only Sim101 controls. Full Console contains diagnostics and advanced controls.</p>
  </main>;
}
