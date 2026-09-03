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
  const commissioning = paper.status?.commissioning_lifecycle;
  // Older running backends classify ARMED_FLAT as "not safely disarmed" even
  // though an intentional one-shot commissioning reservation owns authority.
  // Render that known lifecycle directly so Slim never calls healthy waiting
  // a runtime failure while the backend update is awaiting its next restart.
  const waitingForConfluence = commissioning?.active === true
    && commissioning?.phase === "WAITING_FOR_HIGH_CONFLUENCE"
    && paper.status?.state === "ARMED_FLAT";
  const light = waitingForConfluence ? "YELLOW" : status?.light || "RED";
  const label = waitingForConfluence ? "WAITING FOR HIGH CONFLUENCE" : status?.label || "NOT READY";
  const message = waitingForConfluence
    ? `Commissioning is armed and waiting for a fresh signal meeting ${paper.status?.effective_confidence_threshold || "the configured"} support and ${paper.status?.entry_dominance_margin || "the configured"} dominance.`
    : status?.message || "Waiting for current canonical paper runtime status.";
  const active = waitingForConfluence || status?.paper_active === true;
  const runtimeState = String(paper.status?.state || "");
  const runtimeMayBeActive = paper.status?.operational_paper_session?.active === true
    || ["STARTING", "PAPER_RUNNING", "ENTRY_PENDING", "OPEN_POSITION", "EXIT_PENDING", "PAUSED", "RECONCILING", "FAULTED", "LOCKED_OUT"].includes(runtimeState);
  // Keep the idempotent stop path when authority may exist or current runtime
  // truth is unavailable. A known disarmed red/yellow state is a prerequisite
  // condition for the backend-owned start sequence, not a reason to show STOP.
  const stopAvailable = active || runtimeMayBeActive || (status === null && paper.status === null);
  const autoStart = paper.paperAutoStart;
  const autoStartButton = autoStart?.button || { label: "Checking startup gates…", enabled: false };
  const pnl = status?.pnl || { state: "MISSING" };
  const session = status?.session || {};
  const startEnabled = autoStartButton.enabled === true && autoStart?.in_progress !== true && !paper.autoStartBusy;
  const pnlUnavailable = pnl.state !== "CURRENT";
  const startupStatus = autoStart?.in_progress
    ? autoStartButton.label
    : autoStart?.blockers?.length ? `Blocked: ${autoStart.blockers.join(", ")}`
    : "Launch, sign-in, MNQ observer, reconciliation, and ledger verification are automatic.";

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
      <h1 id="slim-readiness-heading">{label}</h1>
      <p className="slim-message" role="status" aria-live="polite">{message}</p>
      <p className="slim-session">Session: <strong>{session.session_kind || "OFF_SESSION"}</strong>{session.session_family ? ` / ${session.session_family}` : ""}{session.entry_window ? ` · ${session.entry_window}` : ""}</p>
      <p className="slim-session">Profile: <strong>{paper.status?.entry_profile || "UNAVAILABLE"}</strong>{paper.status?.effective_confidence_threshold ? ` · ${paper.status.effective_confidence_threshold} threshold` : ""}</p>
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
      {stopAvailable
        ? <button className="slim-action stop" type="button" disabled={paper.busy} onClick={() => void paper.stopAndDisarm()}>{paper.busy ? "STOPPING…" : "STOP TRADING"}</button>
        : <button className="slim-action start" type="button" disabled={!startEnabled} onClick={() => void paper.startPaperAutoStart()} aria-describedby="slim-startup-status">{paper.autoStartBusy ? "Starting…" : autoStartButton.label}</button>}
      <p id="slim-startup-status" className="slim-verification-result" role="status" aria-live="polite">{startupStatus}</p>
    </section>
    {paper.error && <p className="slim-error" role="alert">Status unavailable — {paper.error}</p>}
    <p className="slim-footnote">Paper-only Sim101 controls. Full Console contains diagnostics and advanced controls.</p>
  </main>;
}
