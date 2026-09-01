import { useCallback, useEffect, useState } from "react";
import { api, post } from "./api";

export type PaperConsoleToast = { tone: "error" | "success" | "warning"; message: string };

export type PaperConsoleState = {
  status: any;
  schedule: any;
  rehearsal: any;
  slimStatus: any;
  error: string | null;
  busy: boolean;
  verificationInFlight: boolean;
  verificationMode: string;
  setVerificationMode: (mode: string) => void;
  act: (path: string, label: string, body?: Record<string, string>) => Promise<any>;
  runRehearsal: () => Promise<void>;
  startCommissioning: () => Promise<void>;
  startPaperTrading: () => Promise<void>;
  startVerification: () => Promise<void>;
  cancelVerification: () => Promise<void>;
  saveSchedule: () => Promise<void>;
  setSchedule: (value: any) => void;
  stopAndDisarm: () => Promise<void>;
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
  const [commissioningRequestId, setCommissioningRequestId] = useState<string | null>(null);
  const [operationalPaperRequestId, setOperationalPaperRequestId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [verificationInFlight, setVerificationInFlight] = useState(false);
  const [verificationMode, setVerificationMode] = useState("auto");

  const load = useCallback(async () => {
    try {
      const [paper, nextSchedule, nextSlim] = await Promise.all([
        api<any>("/api/lane-iii/paper"),
        api<any>("/api/lane-iii/paper/ledger-verification/schedule"),
        includeSlim ? api<any>("/api/lane-iii/paper/slim-status") : Promise.resolve(null),
      ]);
      setStatus(paper);
      setSchedule(nextSchedule);
      setSlimStatus(nextSlim);
      setError(null);
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : "Lane III paper status is unavailable.");
      // No prior paper state may survive a failed refresh in either view.
      // In particular, a stale READY_DISARMED status must not leave a Full
      // Console control looking eligible while its backend is unavailable.
      setStatus(null);
      setSchedule(null);
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

  return {
    status,
    schedule,
    rehearsal,
    slimStatus,
    error,
    busy,
    verificationInFlight,
    verificationMode,
    setVerificationMode,
    act,
    runRehearsal,
    startCommissioning,
    startPaperTrading,
    startVerification,
    cancelVerification,
    saveSchedule,
    setSchedule,
    stopAndDisarm,
  };
}
