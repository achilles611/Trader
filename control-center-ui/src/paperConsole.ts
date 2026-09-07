import { useCallback, useEffect, useState } from "react";
import { api, post } from "./api";

export type PaperConsoleToast = { tone: "error" | "success" | "warning"; message: string };

export type PaperConsoleState = {
  status: any;
  schedule: any;
  rehearsal: any;
  slimStatus: any;
  ninjaTraderMaintenance: any;
  paperAutoStart: any;
  profileSwitch: any;
  error: string | null;
  busy: boolean;
  maintenanceBusy: boolean;
  autoStartBusy: boolean;
  profileSwitchBusy: boolean;
  verificationInFlight: boolean;
  verificationMode: string;
  setVerificationMode: (mode: string) => void;
  act: (path: string, label: string, body?: Record<string, string>) => Promise<any>;
  runRehearsal: () => Promise<void>;
  startCommissioning: () => Promise<void>;
  startPaperTrading: () => Promise<void>;
  startPaperAutoStart: () => Promise<void>;
  switchPaperProfile: (targetProfile: string) => Promise<void>;
  startVerification: () => Promise<void>;
  cancelVerification: () => Promise<void>;
  saveSchedule: () => Promise<void>;
  setSchedule: (value: any) => void;
  stopAndDisarm: () => Promise<void>;
  startNinjaTraderMaintenance: () => Promise<void>;
};

type Options = {
  active: boolean;
  includeSlim: boolean;
  notify: (toast: PaperConsoleToast) => void;
};

const slimTimeoutMilliseconds = 8_000;

export function usePaperConsoleState({ active, includeSlim, notify }: Options): PaperConsoleState {
  const [status, setStatus] = useState<any>(null);
  const [schedule, setSchedule] = useState<any>(null);
  const [rehearsal, setRehearsal] = useState<any>(null);
  const [slimStatus, setSlimStatus] = useState<any>(null);
  const [ninjaTraderMaintenance, setNinjaTraderMaintenance] = useState<any>(null);
  const [paperAutoStart, setPaperAutoStart] = useState<any>(null);
  const [profileSwitch, setProfileSwitch] = useState<any>(null);
  const [maintenanceRequestId, setMaintenanceRequestId] = useState<string | null>(null);
  const [commissioningRequestId, setCommissioningRequestId] = useState<string | null>(null);
  const [operationalPaperRequestId, setOperationalPaperRequestId] = useState<string | null>(null);
  const [autoStartRequestId, setAutoStartRequestId] = useState<string | null>(null);
  const [profileSwitchRequestId, setProfileSwitchRequestId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [maintenanceBusy, setMaintenanceBusy] = useState(false);
  const [autoStartBusy, setAutoStartBusy] = useState(false);
  const [profileSwitchBusy, setProfileSwitchBusy] = useState(false);
  const [verificationInFlight, setVerificationInFlight] = useState(false);
  const [verificationMode, setVerificationMode] = useState("auto");

  const load = useCallback(async () => {
    try {
      const [paper, nextSchedule, nextSlim, maintenance, autoStart, nextProfileSwitch] = await Promise.all([
        api<any>("/api/lane-iii/paper"),
        api<any>("/api/lane-iii/paper/ledger-verification/schedule"),
        includeSlim ? api<any>("/api/lane-iii/paper/slim-status") : Promise.resolve(null),
        api<any>("/api/lane-iii/ninjatrader-maintenance"),
        api<any>("/api/lane-iii/paper/auto-start"),
        api<any>("/api/lane-iii/paper/profile-switch"),
      ]);
      setStatus(paper);
      setSchedule(nextSchedule);
      setSlimStatus(nextSlim);
      setNinjaTraderMaintenance(maintenance);
      setPaperAutoStart(autoStart);
      setProfileSwitch(nextProfileSwitch);
      setError(null);
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : "Lane III paper status is unavailable.");
      // No prior paper state may survive a failed refresh in either view.
      // In particular, a stale READY_DISARMED status must not leave a Full
      // Console control looking eligible while its backend is unavailable.
      setStatus(null);
      setSchedule(null);
      setNinjaTraderMaintenance(null);
      setPaperAutoStart(null);
      setProfileSwitch(null);
      if (includeSlim) setSlimStatus(null);
    }
  }, [includeSlim]);

  useEffect(() => {
    if (!active) {
      setSlimStatus(null);
      return;
    }
    void load();
    const timer = window.setInterval(() => void load(), includeSlim ? 5_000 : 2_000);
    return () => window.clearInterval(timer);
  }, [active, includeSlim, load]);

  useEffect(() => {
    if (!slimStatus?.generated_at) return;
    const generatedAt = slimStatus.generated_at;
    const timer = window.setTimeout(() => {
      setSlimStatus((current: any) => current?.generated_at === generatedAt ? null : current);
    }, slimTimeoutMilliseconds);
    return () => window.clearTimeout(timer);
  }, [slimStatus?.generated_at]);

  useEffect(() => {
    if (!ninjaTraderMaintenance || ninjaTraderMaintenance.in_progress === true) return;
    if (["READY", "BLOCKED", "FAILED", "CANCELLED"].includes(String(ninjaTraderMaintenance.stage))) {
      setMaintenanceRequestId(null);
    }
  }, [ninjaTraderMaintenance]);

  useEffect(() => {
    if (!paperAutoStart || paperAutoStart.in_progress === true) return;
    if (["RUNNING", "BLOCKED", "FAILED", "CANCELLED"].includes(String(paperAutoStart.stage))) {
      setAutoStartRequestId(null);
    }
  }, [paperAutoStart]);

  useEffect(() => {
    if (!profileSwitch || profileSwitch.in_progress === true) return;
    if ([
      "RUNNING", "BLOCKED_SAFE", "RUNNING_SELECTION_PERSISTENCE_FAILED", "FAILED", "CANCELLED",
    ].includes(String(profileSwitch.stage))) {
      setProfileSwitchRequestId(null);
    }
  }, [profileSwitch]);

  const act = useCallback(async (path: string, label: string, body?: Record<string, string>) => {
    if (busy) return undefined;
    setBusy(true);
    try {
      const result = await post<any>(path, body);
      await load();
      notify({
        tone: result.armed === false || result.paused === false || result.resumed === false || result.submitted === false ? "warning" : "success",
        message: `${label}: ${String(result.state || "recorded")}`,
      });
      return result;
    } catch (failure) {
      notify({ tone: "error", message: failure instanceof Error ? failure.message : `${label} failed.` });
      await load();
      return undefined;
    } finally {
      setBusy(false);
    }
  }, [busy, load, notify]);

  const runRehearsal = useCallback(async () => {
    const result = await act("/api/lane-iii/paper/commissioning-rehearsal", "Commissioning Rehearsal");
    if (result) setRehearsal(result);
  }, [act]);

  const startCommissioning = useCallback(async () => {
    const requestId = commissioningRequestId || `l3g-ui-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setCommissioningRequestId(requestId);
    const result = await act("/api/lane-iii/paper/commissioning-start", "Atomic Commissioning Start", { request_id: requestId });
    if (result?.submitted || result?.idempotent_replay) setCommissioningRequestId(null);
  }, [act, commissioningRequestId]);

  const startPaperTrading = useCallback(async () => {
    const requestId = operationalPaperRequestId || `l3g-paper-ui-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setOperationalPaperRequestId(requestId);
    const result = await act("/api/lane-iii/paper/operational-start", "Start Paper Trading", { request_id: requestId });
    if (result?.started || result?.idempotent_replay) setOperationalPaperRequestId(null);
  }, [act, operationalPaperRequestId]);

  const startPaperAutoStart = useCallback(async () => {
    if (autoStartBusy || paperAutoStart?.in_progress === true) return;
    if (typeof paperAutoStart?.action_token !== "string" || !paperAutoStart.action_token) {
      notify({ tone: "error", message: "Paper auto-start authentication is unavailable." });
      return;
    }
    const requestId = autoStartRequestId || `paper-auto-ui-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setAutoStartRequestId(requestId);
    setAutoStartBusy(true);
    try {
      const result = await api<any>("/api/lane-iii/paper/auto-start", {
        method: "POST",
        headers: {
          "X-Beelzebub-Paper-Autostart-Action": "sim101-paper-autostart-v1",
          "X-Beelzebub-Paper-Autostart-Token": paperAutoStart.action_token,
        },
        body: JSON.stringify({ request_id: requestId }),
      });
      setPaperAutoStart(result);
      notify({ tone: "success", message: "Paper startup requested. Safety gates remain authoritative." });
      await load();
    } catch (failure) {
      notify({ tone: "error", message: failure instanceof Error ? failure.message : "Paper auto-start could not begin." });
      await load();
    } finally {
      setAutoStartBusy(false);
    }
  }, [autoStartBusy, autoStartRequestId, load, notify, paperAutoStart?.action_token, paperAutoStart?.in_progress]);

  const switchPaperProfile = useCallback(async (targetProfile: string) => {
    if (profileSwitchBusy || profileSwitch?.in_progress === true) return;
    if (typeof profileSwitch?.action_token !== "string" || !profileSwitch.action_token) {
      notify({ tone: "error", message: "Profile-switch authentication is unavailable." });
      return;
    }
    const requestId = profileSwitchRequestId || `profile-switch-ui-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setProfileSwitchRequestId(requestId);
    setProfileSwitchBusy(true);
    try {
      const result = await api<any>("/api/lane-iii/paper/profile-switch", {
        method: "POST",
        headers: {
          "X-Beelzebub-Profile-Switch-Action": "sim101-profile-switch-v1",
          "X-Beelzebub-Profile-Switch-Token": profileSwitch.action_token,
        },
        body: JSON.stringify({ request_id: requestId, target_profile: targetProfile }),
      });
      setProfileSwitch(result);
      const stage = String(result?.stage || "");
      const blockers = Array.isArray(result?.blockers) ? result.blockers.map(String) : [];
      if (result?.target_profile !== targetProfile) {
        notify({ tone: "error", message: "Profile switch was not accepted: another target owns the current operation." });
      } else if (stage === "RUNNING_SELECTION_PERSISTENCE_FAILED") {
        notify({
          tone: "error",
          message: `Profile target is running, but the remembered selection was not persisted: ${blockers.length ? blockers.join(", ") : "backend reported no persistence detail"}.`,
        });
      } else if (["BLOCKED_SAFE", "FAILED", "CANCELLED"].includes(stage) || result?.in_progress !== true) {
        notify({
          tone: "error",
          message: `Profile switch did not begin: ${blockers.length ? blockers.join(", ") : stage || "backend returned no active operation"}.`,
        });
      } else {
        notify({ tone: "success", message: "Profile switch accepted. Beelzebub is closing the current ledger before restart." });
      }
    } catch (failure) {
      notify({ tone: "error", message: failure instanceof Error ? failure.message : "Profile switching could not begin." });
      await load();
    } finally {
      setProfileSwitchBusy(false);
    }
  }, [load, notify, profileSwitch, profileSwitchBusy, profileSwitchRequestId]);

  const startVerification = useCallback(async () => {
    if (busy) return;
    setBusy(true);
    setVerificationInFlight(true);
    try {
      const result = await post<any>("/api/lane-iii/paper/ledger-verification", { mode: verificationMode });
      await load();
      notify({ tone: "success", message: result.status === "IN_PROGRESS" ? "Local ledger verification is running." : `Ledger verification: ${result.status}.` });
    } catch (failure) {
      notify({ tone: "error", message: failure instanceof Error ? failure.message : "Ledger verification could not start." });
      await load();
    } finally {
      setVerificationInFlight(false);
      setBusy(false);
    }
  }, [busy, load, notify, verificationMode]);

  const cancelVerification = useCallback(async () => {
    if (busy) return;
    setBusy(true);
    try {
      await post<any>("/api/lane-iii/paper/ledger-verification/cancel");
      await load();
      notify({ tone: "warning", message: "Local ledger verification cancellation requested." });
    } catch (failure) {
      notify({ tone: "error", message: failure instanceof Error ? failure.message : "Ledger verification could not be cancelled." });
      await load();
    } finally {
      setBusy(false);
    }
  }, [busy, load, notify]);

  const saveSchedule = useCallback(async () => {
    if (busy || !schedule) return;
    setBusy(true);
    try {
      const saved = await post<any>("/api/lane-iii/paper/ledger-verification/schedule", schedule);
      setSchedule(saved);
      notify({ tone: "success", message: saved.enabled ? "Local ledger verification schedule saved." : "Local ledger verification schedule disabled." });
    } catch (failure) {
      notify({ tone: "error", message: failure instanceof Error ? failure.message : "Ledger verification schedule could not be saved." });
    } finally {
      setBusy(false);
    }
  }, [busy, notify, schedule]);

  const stopAndDisarm = useCallback(async () => {
    await act("/api/lane-iii/paper/flatten-and-disarm", "Stop & Disarm");
  }, [act]);

  const startNinjaTraderMaintenance = useCallback(async () => {
    if (maintenanceBusy || ninjaTraderMaintenance?.in_progress === true) return;
    if (typeof ninjaTraderMaintenance?.action_token !== "string" || !ninjaTraderMaintenance.action_token) {
      notify({ tone: "error", message: "NinjaTrader maintenance authentication is unavailable." });
      return;
    }
    const requestId = maintenanceRequestId || `ntm-ui-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setMaintenanceRequestId(requestId);
    setMaintenanceBusy(true);
    try {
      const result = await api<any>("/api/lane-iii/ninjatrader-maintenance", {
        method: "POST",
        headers: {
          "X-Beelzebub-Maintenance-Action": "ninjatrader-observer-repair-v1",
          "X-Beelzebub-Maintenance-Token": ninjaTraderMaintenance.action_token,
        },
        body: JSON.stringify({ request_id: requestId }),
      });
      setNinjaTraderMaintenance(result);
      notify({ tone: "success", message: "NinjaTrader observer maintenance started." });
      await load();
    } catch (failure) {
      notify({ tone: "error", message: failure instanceof Error ? failure.message : "NinjaTrader maintenance could not start." });
      await load();
    } finally {
      setMaintenanceBusy(false);
    }
  }, [load, maintenanceBusy, maintenanceRequestId, ninjaTraderMaintenance?.action_token, ninjaTraderMaintenance?.in_progress, notify]);

  return {
    status,
    schedule,
    rehearsal,
    slimStatus,
    ninjaTraderMaintenance,
    paperAutoStart,
    profileSwitch,
    error,
    busy,
    maintenanceBusy,
    autoStartBusy,
    profileSwitchBusy,
    verificationInFlight,
    verificationMode,
    setVerificationMode,
    act,
    runRehearsal,
    startCommissioning,
    startPaperTrading,
    startPaperAutoStart,
    switchPaperProfile,
    startVerification,
    cancelVerification,
    saveSchedule,
    setSchedule,
    stopAndDisarm,
    startNinjaTraderMaintenance,
  };
}
