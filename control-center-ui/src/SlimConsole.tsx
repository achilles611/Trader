import { useEffect, useRef, useState } from "react";
import type { PaperConsoleState } from "./paperConsole";

const PAPER_PROFILE_PREFERENCE = "beezconsole-paper-profile-selection";
const FIVE_MINUTE_PERPETUAL_PROFILE = "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2";

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
  const runtimeState = String(paper.status?.state || "");
  const runtimeMayBeActive = paper.status?.operational_paper_session?.active === true
    || ["STARTING", "PAPER_RUNNING", "ENTRY_PENDING", "OPEN_POSITION", "LONG", "SHORT", "EXIT_PENDING", "STOPPING", "PAUSED", "RECONCILING", "FAULTED", "LOCKED_OUT"].includes(runtimeState);
  const positionRequirement = paper.status?.position_requirement;
  const sourceSignal = positionRequirement?.source_signal;
  const perpetualRuntimeActive = paper.status?.entry_profile_version === FIVE_MINUTE_PERPETUAL_PROFILE
    && (runtimeMayBeActive || status?.paper_active === true);
  const perpetualRuntimeFlat = perpetualRuntimeActive
    && (paper.status?.current_position === "FLAT" || positionRequirement?.actual_position === "FLAT");
  const requirementReasonsSupplied = Array.isArray(positionRequirement?.blocking_reasons);
  const requirementReasons = requirementReasonsSupplied
    ? positionRequirement.blocking_reasons.filter((reason: unknown) => typeof reason === "string" && reason.trim())
    : [];
  const requirementReasonsEmpty = requirementReasonsSupplied
    && positionRequirement.blocking_reasons.length === 0;
  const positionedDirection = ["LONG", "SHORT"].includes(runtimeState) ? runtimeState : null;
  const runtimeIdentityProven = paper.status?.mode === "PAPER_SIM101"
    && paper.status?.paper_account === "Sim101"
    && paper.status?.account_class === "LOCAL_SIMULATION"
    && paper.status?.market_instrument === "MNQ SEP26"
    && paper.status?.maximum_quantity === 1
    && paper.status?.live_capital === "DENIED";
  const runtimePositionProven = positionedDirection !== null
    && paper.status?.paper_execution === "POSITIONED"
    && paper.status?.current_position === positionedDirection
    && paper.status?.current_quantity === 1
    && paper.status?.current_position_quantity === 1
    && paper.status?.broker_snapshot_position === positionedDirection
    && paper.status?.broker_snapshot_position_quantity === 1;
  const requirementProven = positionRequirement?.required === true
    && positionRequirement?.state === "POSITIONED"
    && positionRequirement?.actual_position === positionedDirection
    && positionRequirement?.actual_quantity === 1
    && positionRequirement?.desired_position === positionedDirection
    && positionRequirement?.primary_blocker == null
    && requirementReasonsEmpty
    && sourceSignal?.direction === positionedDirection
    && typeof sourceSignal?.candle_close_utc === "string"
    && sourceSignal.candle_close_utc.trim().length > 0
    && typeof sourceSignal?.signal_hash === "string"
    && sourceSignal.signal_hash.trim().length > 0
    && Number.isInteger(sourceSignal?.ledger_sequence)
    && sourceSignal.ledger_sequence > 0
    && typeof sourceSignal?.record_hash === "string"
    && sourceSignal.record_hash.trim().length > 0
    && sourceSignal?.ledger_verified === true;
  const reconciliationProven = paper.status?.position_snapshot_complete === true
    && paper.status?.order_snapshot_complete === true
    && paper.status?.reconciliation_current === true
    && paper.status?.unresolved_command === false
    && paper.status?.unresolved_native_order === false
    && paper.status?.unresolved_execution === false;
  const perpetualPositionProven = perpetualRuntimeActive
    && runtimeIdentityProven
    && runtimePositionProven
    && paper.status?.working_owned_orders === 1
    && paper.status?.working_entry_orders === 0
    && paper.status?.foreign_activity === false
    && paper.status?.protective_stop_state === "WORKING"
    && (paper.status?.lockout_or_fault_reason == null || paper.status?.lockout_or_fault_reason === "")
    && reconciliationProven
    && requirementProven;
  const perpetualProjectionUnproved = perpetualRuntimeActive && !perpetualPositionProven;
  const suppliedRuntimeBlocker = typeof positionRequirement?.primary_blocker === "string"
    && positionRequirement.primary_blocker.trim()
    ? positionRequirement.primary_blocker.trim()
    : requirementReasons.length ? String(requirementReasons[0]).trim()
    : null;
  const exactRuntimeBlocker = suppliedRuntimeBlocker
    || (perpetualRuntimeFlat ? "PERPETUAL_POSITION_REQUIREMENT_UNAVAILABLE"
    : !runtimeIdentityProven ? "ACTIVE_RUNTIME_IDENTITY_UNHEALTHY"
    : !runtimePositionProven ? "ACTIVE_POSITION_UNHEALTHY"
    : paper.status?.protective_stop_state !== "WORKING" ? "PROTECTIVE_STOP_REJECTED"
    : !requirementProven ? "ACTIVE_POSITION_REQUIREMENT_UNHEALTHY"
    : paper.status?.working_owned_orders !== 1 ? "ACTIVE_OWNED_ORDER_SET_UNHEALTHY"
    : paper.status?.working_entry_orders !== 0 ? "ACTIVE_WORKING_ENTRY_ORDER"
    : paper.status?.foreign_activity !== false ? "FOREIGN_ACTIVITY_LOCKOUT"
    : typeof paper.status?.lockout_or_fault_reason === "string" && paper.status.lockout_or_fault_reason.trim()
    ? paper.status.lockout_or_fault_reason.trim()
    : !reconciliationProven ? "RECONCILIATION_INCOMPLETE"
    : "PERPETUAL_POSITION_REQUIREMENT_UNAVAILABLE");
  // Older running backends classify ARMED_FLAT as "not safely disarmed" even
  // though an intentional one-shot commissioning reservation owns authority.
  // Render that known lifecycle directly so Slim never calls healthy waiting
  // a runtime failure while the backend update is awaiting its next restart.
  const waitingForProfileSignal = commissioning?.active === true
    && commissioning?.phase === "WAITING_FOR_PROFILE_SIGNAL"
    && paper.status?.state === "ARMED_FLAT";
  // `/paper` and `/slim-status` are separate reads. If a fill, exit, or
  // reconciliation lands between them, current V2 position truth must override
  // a stale green compact projection until the next poll catches up.
  const light = perpetualProjectionUnproved ? "RED" : waitingForProfileSignal ? "YELLOW" : status?.light || "RED";
  const label = perpetualProjectionUnproved
    ? perpetualRuntimeFlat
      ? `FLAT — BLOCKED: ${exactRuntimeBlocker}`
      : `POSITION UNPROVEN — BLOCKED: ${exactRuntimeBlocker}`
    : waitingForProfileSignal ? "WAITING FOR PROFILE SIGNAL" : status?.label || "NOT READY";
  const message = perpetualProjectionUnproved
    ? status?.primary_blocker === exactRuntimeBlocker && status?.light !== "GREEN"
      ? status?.message || "The perpetual-position requirement is blocked; review Full Console diagnostics."
      : "The perpetual-position requirement is blocked; review Full Console diagnostics."
    : waitingForProfileSignal
    ? `Commissioning is armed and waiting for a fresh profile-qualified signal meeting ${paper.status?.effective_confidence_threshold || "the configured"} support and ${paper.status?.entry_dominance_margin || "the configured"} dominance.`
    : status?.message || "Waiting for current canonical paper runtime status.";
  const active = waitingForProfileSignal || status?.paper_active === true;
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
  const autoWarmup = autoStart?.warmup || {};
  const autoWarmupDetail = [
    autoStart?.blockers?.length ? `Blockers: ${autoStart.blockers.join(", ")}` : "",
    autoWarmup?.missing_families?.length
      ? `Missing: ${autoWarmup.missing_families.join(", ")}` : "",
    typeof autoWarmup?.elapsed_seconds === "number"
      ? `Elapsed: ${autoWarmup.elapsed_seconds}s / ${autoWarmup.timeout_seconds ?? "?"}s` : "",
  ].filter(Boolean).join(" · ");
  const startupStatus = autoStart?.in_progress
    ? `${autoStartButton.label}${autoWarmupDetail ? ` — ${autoWarmupDetail}` : ""}`
    : autoStart?.blockers?.length ? `Blocked: ${autoStart.blockers.join(", ")}`
    : "Launch, sign-in, MNQ observer, reconciliation, and ledger verification are automatic.";
  const profileSwitch = paper.profileSwitch;
  const profiles = Array.isArray(profileSwitch?.profiles) ? profileSwitch.profiles : [];
  const activeProfile = String(profileSwitch?.active_profile || paper.status?.entry_profile_version || "");
  const operationTarget = String(profileSwitch?.target_profile || "");
  const requestedProfile = String(profileSwitch?.selection?.requested?.profile || operationTarget || "");
  const establishedProfile = String(profileSwitch?.selection?.established?.profile || activeProfile || "");
  const [selectedProfile, setSelectedProfile] = useState(() => {
    try { return window.localStorage.getItem(PAPER_PROFILE_PREFERENCE) || ""; }
    catch { return ""; }
  });
  const authoritativeSelection = `${String(profileSwitch?.operation_id || "")}|${String(profileSwitch?.stage || "IDLE")}|${activeProfile}|${requestedProfile}|${establishedProfile}`;
  const lastAuthoritativeSelection = useRef("");
  useEffect(() => {
    if (!profiles.length) return;
    const selectedExists = profiles.some((profile: any) => profile.selection_key === selectedProfile);
    if (lastAuthoritativeSelection.current === authoritativeSelection && selectedExists) return;
    lastAuthoritativeSelection.current = authoritativeSelection;
    const operationExists = profiles.some((profile: any) => profile.selection_key === operationTarget);
    const establishedExists = profiles.some((profile: any) => profile.selection_key === establishedProfile);
    const activeExists = profiles.some((profile: any) => profile.selection_key === activeProfile);
    const backendSelection = profileSwitch?.operation_id && operationExists
      ? operationTarget
      : establishedExists ? establishedProfile
      : activeExists ? activeProfile
      : String(profiles[0].selection_key);
    if (selectedProfile !== backendSelection) setSelectedProfile(backendSelection);
  }, [activeProfile, authoritativeSelection, establishedProfile, operationTarget, profileSwitch?.operation_id, profiles, selectedProfile]);
  useEffect(() => {
    if (!selectedProfile || !profiles.some((profile: any) => profile.selection_key === selectedProfile)) return;
    try { window.localStorage.setItem(PAPER_PROFILE_PREFERENCE, selectedProfile); }
    catch { /* preference persistence is optional */ }
  }, [profiles, selectedProfile]);
  const targetProfile = selectedProfile || activeProfile;
  const switchInProgress = profileSwitch?.in_progress === true;
  const switchStage = String(profileSwitch?.stage || "");
  const exactPositionedTarget = ["LONG", "SHORT"].includes(runtimeState)
    && paper.status?.paper_execution === "POSITIONED"
    && paper.status?.current_position === runtimeState
    && paper.status?.current_quantity === 1
    && paper.status?.current_position_quantity === 1
    && paper.status?.broker_snapshot_position === runtimeState
    && paper.status?.broker_snapshot_position_quantity === 1;
  const targetRuntimeRunning = (
    runtimeState === "PAPER_RUNNING" && paper.status?.paper_execution === "RUNNING"
  ) || exactPositionedTarget;
  const runningTargetHasStaleSwitchProjection = switchInProgress
    && operationTarget === activeProfile
    && paper.status?.entry_profile_version === activeProfile
    && targetRuntimeRunning
    && paper.status?.operational_paper_session?.active === true
    && autoStart?.stage === "RUNNING"
    && autoStart?.in_progress !== true;
  const switchBlocked = ["BLOCKED_SAFE", "FAILED", "CANCELLED", "RUNNING_SELECTION_PERSISTENCE_FAILED"].includes(switchStage);
  const switchStatus = runningTargetHasStaleSwitchProjection
    ? "Selected profile is running. The prior switch projection is stale."
    : switchInProgress
    ? `Switch in progress: ${String(profileSwitch.stage || "PREPARING").replaceAll("_", " ")}${profileSwitch?.blockers?.length ? ` — ${profileSwitch.blockers.join(", ")}` : ""}`
    : switchStage === "RUNNING_SELECTION_PERSISTENCE_FAILED"
    ? `Target is running, but its remembered selection was not durably established: ${profileSwitch?.blockers?.join(", ") || "PROFILE_SELECTION_PERSISTENCE_FAILED"}`
    : switchBlocked
    ? `Switch blocked safely: ${profileSwitch?.blockers?.length ? profileSwitch.blockers.join(", ") : String(profileSwitch?.stage)}`
    : profileSwitch?.stage === "RUNNING"
    ? `Current profile is running: ${activeProfile}.`
    : "A switch flattens the current profile, seals its ledger and risk counters, creates a fresh run, then verifies and starts the target automatically.";

  return <main className="slim-console" aria-label="BeezConsole Slim Mode">
    <header className="slim-header">
      <div className="slim-brand"><span className="slim-brand-mark" aria-hidden="true">B</span><span>BeezConsole <b>Slim</b></span></div>
      <button className="slim-mode-button" type="button" onClick={onFullConsole}>Full Console</button>
    </header>

    <section className="slim-card slim-profile" aria-labelledby="slim-profile-heading">
      <div className="slim-kicker">PAPER PROFILE</div>
      <h2 id="slim-profile-heading">Select profile</h2>
      <div className="slim-profile-controls">
        <select
          aria-label="Paper profile"
          value={targetProfile}
          disabled={!profiles.length || switchInProgress || paper.profileSwitchBusy}
          onChange={(event) => setSelectedProfile(event.target.value)}
        >
          {profiles.map((profile: any) => <option key={profile.selection_key} value={profile.selection_key}>{profile.display_name}</option>)}
        </select>
        <button
          className="slim-profile-switch"
          type="button"
          disabled={!targetProfile || targetProfile === activeProfile || switchInProgress || paper.profileSwitchBusy}
          onClick={() => void paper.switchPaperProfile(targetProfile)}
        >
          {paper.profileSwitchBusy ? "Preparing…" : runningTargetHasStaleSwitchProjection ? "Running" : switchInProgress ? "Switching…" : "Switch & Start"}
        </button>
      </div>
      <p className={`slim-verification-result ${switchBlocked ? "negative" : ""}`} role="status" aria-live="polite">{switchStatus}</p>
      <p className="slim-profile-current">Current: <strong>{profiles.find((profile: any) => profile.selection_key === activeProfile)?.display_name || activeProfile || "UNAVAILABLE"}</strong></p>
      <p className="slim-profile-current">Requested: <strong>{profiles.find((profile: any) => profile.selection_key === requestedProfile)?.display_name || requestedProfile || "NONE"}</strong></p>
      <p className="slim-profile-current">Last established: <strong>{profiles.find((profile: any) => profile.selection_key === establishedProfile)?.display_name || establishedProfile || "NONE"}</strong></p>
      {profileSwitch?.selection?.status === "INVALID" && <p className="slim-verification-result negative" role="alert">Remembered profile state is invalid; startup remains blocked.</p>}
    </section>

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
        ? <button className="slim-action stop" type="button" disabled={paper.busy || switchInProgress} onClick={() => void paper.stopAndDisarm()}>{paper.busy ? "STOPPING…" : "STOP TRADING"}</button>
        : <button className="slim-action start" type="button" disabled={!startEnabled || switchInProgress} onClick={() => void paper.startPaperAutoStart()} aria-describedby="slim-startup-status">{paper.autoStartBusy ? "Starting…" : autoStartButton.label}</button>}
      <p id="slim-startup-status" className="slim-verification-result" role="status" aria-live="polite">{startupStatus}</p>
    </section>
    {paper.error && <p className="slim-error" role="alert">Status unavailable — {paper.error}</p>}
    <p className="slim-footnote">Paper-only Sim101 controls. Full Console contains diagnostics and advanced controls.</p>
  </main>;
}
