import { useState } from "react";
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
  const [confirmBeeztmode, setConfirmBeeztmode] = useState(false);
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
  const pnl = status?.pnl || { state: "MISSING" };
  const verificationRunning = verification.state === "IN_PROGRESS" || paper.verificationInFlight;
  const startEnabled = status?.can_start === true && !paper.busy;
  const profile = paper.status?.entry_profile || {};
  const beeztmodeActive = profile.selected_profile === "BEEZTMODE_V1";
  const profileSelectionEnabled = profile.selection_permitted === true && !paper.busy;
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
    </section>

    <section className="slim-card slim-pnl" aria-labelledby="slim-pnl-heading">
      <div className="slim-kicker">PAPER / SESSION P&amp;L</div>
      <h2 id="slim-pnl-heading" className={pnlUnavailable ? "neutral" : pnlClass(pnl.total)}>{pnlUnavailable ? (pnl.state === "STALE" ? "STALE" : "—") : money(pnl.total)}</h2>
      <dl>
        <div><dt>Realized</dt><dd className={pnlUnavailable ? "neutral" : pnlClass(pnl.realized)}>{pnlUnavailable ? "—" : money(pnl.realized)}</dd></div>
        <div><dt>Unrealized</dt><dd className={pnlUnavailable ? "neutral" : pnlClass(pnl.unrealized)}>{pnlUnavailable ? "—" : money(pnl.unrealized)}</dd></div>
      </dl>
    </section>

    <section className={`slim-card slim-beeztmode ${beeztmodeActive ? "active" : ""}`} aria-labelledby="slim-beeztmode-heading">
      <div className="slim-profile-heading">
        <div><div className="slim-kicker">EXPERIMENTAL PAPER PROFILE</div><h2 id="slim-beeztmode-heading">Beeztmode</h2></div>
        <span className={`beeztmode-badge ${beeztmodeActive ? "active" : "standard"}`}>{beeztmodeActive ? "BEEZTMODE ACTIVE" : "STANDARD"}</span>
      </div>
      <p className="slim-profile-copy">{beeztmodeActive ? "Aggressive paper profile" : "Standard paper profile"}</p>
      <div className="slim-profile-row"><span>Effective confidence threshold</span><strong>{profile.effective_threshold ?? "—"}</strong></div>
      <button
        className={`beeztmode-toggle ${beeztmodeActive ? "enabled" : ""}`}
        type="button"
        role="switch"
        aria-checked={beeztmodeActive}
        disabled={!profileSelectionEnabled}
        onClick={() => beeztmodeActive ? void paper.selectEntryProfile("STANDARD") : setConfirmBeeztmode(true)}
      ><span aria-hidden="true" />{beeztmodeActive ? "Use Standard" : "Enable Beeztmode"}</button>
      {!profileSelectionEnabled && <p className="slim-profile-reason">{profile.selection_reason || "Profile selection is unavailable."}</p>}
      {confirmBeeztmode && <div className="slim-profile-confirm" role="dialog" aria-modal="true" aria-labelledby="slim-beeztmode-confirm-title">
        <strong id="slim-beeztmode-confirm-title">Enable Beeztmode?</strong>
        <p>Beeztmode lowers the paper entry-confidence threshold and may increase trade frequency. All safety and risk gates remain active.</p>
        <div><button type="button" onClick={() => setConfirmBeeztmode(false)}>Cancel</button><button type="button" className="confirm" onClick={() => { setConfirmBeeztmode(false); void paper.selectEntryProfile("BEEZTMODE_V1"); }}>Enable Beeztmode</button></div>
      </div>}
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
