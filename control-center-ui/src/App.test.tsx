import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";

class WebSocketStub {
  static instances: WebSocketStub[] = [];
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: (() => void) | null = null;
  close() {}
  constructor(_url: string) { WebSocketStub.instances.push(this); }
}

const candidate = {
  wallet: "0x1111111111111111111111111111111111111111", operator_state: "shadow", research_state: "qualified",
  score: 87.3, qualified: true, stale_analysis: true, target_net_pnl: 12, follower_net_pnl: 9,
  win_rate: 0.62, profit_factor: 1.8, target_max_drawdown: 0.08, follower_max_drawdown: 0.1, coverage: "PROVEN_COMPLETE", source_count: 2,
};

let closeAllResponse: Record<string, unknown> | null = null;
let emptyUniverse = false;
let filteredEmpty = false;
let discoveryJobResponse: Record<string, unknown> | null = null;
let discoveryJobDetailResponse: Record<string, unknown> | null = null;
let shadowResponse: Record<string, unknown> = { configured: false, state: "NOT_CONFIGURED", freshness: "UNKNOWN" };
let ledgerVerificationResponse: Record<string, unknown> = { status: "UNVERIFIED", full_scan_required: true, chain_valid: false, checkpoint_valid: false, errors: [{ code: "NO_VERIFICATION_ARTIFACT", message: "No completed local ledger verification exists." }] };
let ledgerVerificationSchedule: Record<string, unknown> = { enabled: false, frequency: "DISABLED", local_time: "03:00", weekday: 0, mode: "auto" };
let laneIIIPaperOverrides: Record<string, unknown> = {};
let laneIIILedgerOverrides: Record<string, unknown> = {};
let commissioningRehearsalResponse: Record<string, unknown> | null = null;
let commissioningRehearsalSideEffect: (() => void) | null = null;
let commissioningStartResponse: Record<string, unknown> | null = null;
let commissioningStartSideEffect: (() => void) | null = null;
let operationalStartResponse: Record<string, unknown> | null = null;
let operationalStartSideEffect: (() => void) | null = null;
let slimStatusResponse: Record<string, unknown> = {
  generated_at: new Date().toISOString(), light: "RED", label: "NOT READY",
  message: "Paper runtime status is unavailable.", can_start: false, paper_active: false,
  ledger_verification: { state: "UNAVAILABLE", message: "Verification status is unavailable." },
  pnl: { state: "MISSING", total: null, realized: null, unrealized: null },
};
let accountBalancesResponse: Record<string, unknown> = {
  accounts: {
    Sim101: { cash_value: 100123.45, cash_value_observed_at: "2026-08-29T15:00:00Z" },
    Lucid25kflex01: { cash_value: 24987.65, cash_value_observed_at: "2026-08-29T15:00:00Z" },
  },
};
let laneIIILiveResponse: Record<string, unknown> = {
  terminal_status: "BLOCKED_LIVE_ACCOUNT_IDENTITY", state: "AUTHORIZATION_BOUNDARY_IMPLEMENTED",
  mechanical_commissioning: "COMMISSIONED", live_account_identity: "UNVERIFIED",
  authorization_boundary: "IMPLEMENTED", live_authority: "DISARMED", live_canary: "NOT_RUN",
  account_class: "UNKNOWN", authorized_account: null, live_capital: "DENIED",
  contract: "MNQ SEP26", maximum_quantity: 1, preflight_age_seconds: null,
  authorization_expires_at: null, gateway: "AUTHENTICATED_LOOPBACK", addon_provenance: "PASS",
  reconciliation: "PASS", protection: "PASS", kill_paths: "PASS", quarantine: false, locked: false,
  live_send_count: 0, one_control_start: { enabled: false, reason: "LIVE_ACCOUNT_IDENTITY_UNVERIFIED" },
  components: { LIVE_AUTHORITY: { state: "RED", reason: "DISARMED" } },
};

function payload(path: string) {
  if (path.startsWith("/api/overview")) return { counts: { total_discovered: emptyUniverse ? 0 : 20, qualified: 2, shadow: 1, active: 0 }, funnel: [], top_candidates: [candidate], recent_activity: [] };
  if (path.startsWith("/api/portfolio")) return { equity: 210, cash: 190, committed_capital: 10, open_pnl: 1, realized_pnl_total: 9, max_drawdown: 0.02, open_positions: 1 };
  if (path.startsWith("/api/accounts/balances")) return accountBalancesResponse;
  if (path.startsWith("/api/lane-iii/live")) return laneIIILiveResponse;
  if (path.startsWith("/api/controls/close-all-paper-positions")) return closeAllResponse || { status: "completed", control: { state: "RUNNING", entries_allowed: true, paper_only: true } };
  if (path.startsWith("/api/controls")) return { state: "RUNNING", entries_allowed: true, paper_only: true };
  if (path.startsWith("/api/discovery/status")) return { candidate_universe_count: emptyUniverse ? 0 : 20, source: { source: "Official HyperCore node data", connection_state: "SETUP REQUIRED", aws_credentials_detected: false, requester_pays_access: "not tested", message: "No usable AWS credentials were detected on this machine. No credentials are stored by Trader.", cache: { object_count: 0, size_bytes: 0 } }, presets: { quick: { window_hours: 1, candidate_limit: 1000, min_activity: 2, max_activity_age: "30d" }, standard: { window_hours: 6, candidate_limit: 2500, min_activity: 2, max_activity_age: "30d" }, deep: { window_hours: 24, candidate_limit: 5000, min_activity: 2, max_activity_age: "30d" } } };
  if (path.startsWith("/api/discovery/source/test")) return { source: "Official HyperCore node data", connection_state: "READY", aws_credentials_detected: true, requester_pays_access: "ready", cache: { object_count: 1, size_bytes: 42 } };
  if (path.startsWith("/api/discovery/jobs/")) return discoveryJobDetailResponse || { job_id: "discovery-1", status: "queued", stage: "queued", configuration: { preset: "standard", candidate_limit: 2500 } };
  if (path.startsWith("/api/discovery/jobs")) return discoveryJobResponse || { job_id: "discovery-1", status: "queued", stage: "queued", configuration: { preset: "standard", candidate_limit: 2500 } };
  if (path.startsWith("/api/execution/shadow/refresh")) return shadowResponse;
  if (path.startsWith("/api/execution")) return { shadow: shadowResponse };
  if (path.startsWith("/api/lane-iii/paper/ledger-verification/schedule")) return ledgerVerificationSchedule;
  if (path.startsWith("/api/lane-iii/paper/ledger-verification/cancel")) return { ...ledgerVerificationResponse, cancellation_requested: true };
  if (path.startsWith("/api/lane-iii/paper/ledger-verification")) return ledgerVerificationResponse;
  if (path.startsWith("/api/lane-iii/paper/slim-status")) return slimStatusResponse;
  if (path.startsWith("/api/lane-iii/paper/commissioning-rehearsal")) {
    commissioningRehearsalSideEffect?.();
    return commissioningRehearsalResponse || { result: "BLOCKED", blocking_reasons: ["FIXTURE_BLOCKED"] };
  }
  if (path.startsWith("/api/lane-iii/paper/commissioning-start")) {
    commissioningStartSideEffect?.();
    return commissioningStartResponse || { submitted: false, state: "READY_DISARMED" };
  }
  if (path.startsWith("/api/lane-iii/paper/operational-start")) {
    operationalStartSideEffect?.();
    return operationalStartResponse || { started: false, state: "READY_DISARMED" };
  }
  if (path.startsWith("/api/lane-iii/paper")) return {
    state: "READY_DISARMED",
    ledger_verification: ledgerVerificationResponse,
    ledger: {
      path: "E:\\BeelzebubData\\Hot\\LaneIII\\Epoch-002\\lane_iii_paper.sqlite3",
      epoch_id: "L3G-PAPER-EPOCH-002",
      file_size: 4096,
      free_bytes: 200000000000,
      quick_check_state: "ok",
      chain_valid: true,
      broken_identity: null,
      highest_sequence: 125,
      verified_through_sequence: 100,
      unverified_tail_rows: 25,
      commissioning_ledger_state: "UNVERIFIED_AUTHORITY_TAIL",
      last_authority_mutation_sequence: 120,
      last_authority_mutation_domain: "RISK_EVENT",
      last_authority_mutation_kind: "RISK_LOCKOUT",
      last_authority_observation_sequence: 124,
      last_authority_observation_domain: "POSITION_SNAPSHOT",
      last_authority_observation_kind: "BROKER_STATE_OBSERVED",
      last_unknown_sequence: 125,
      last_unknown_domain: "FUTURE_DOMAIN",
      last_unknown_kind: "FUTURE_KIND",
      last_blocking_classification: "UNKNOWN",
      last_blocking_sequence: 125,
      last_blocking_domain: "FUTURE_DOMAIN",
      last_blocking_kind: "FUTURE_KIND",
      last_blocking_payload: "DO_NOT_RENDER_TAIL_PAYLOAD",
      last_record_time: "2026-08-25T00:30:00Z",
      wal_size: 1024,
      ...laneIIILedgerOverrides,
    },
    ...laneIIIPaperOverrides,
  };
  if (path.startsWith("/api/system")) return { health: { mode: "paper", paper_only: true, database: { connected: true }, websocket: { available: true }, watcher: { state: "NOT_ATTACHED", desired_target_count: 0, subscribed_target_count: 0, membership_in_sync: true }, recovery: { wallets: [{ wallet: candidate.wallet, state: "RECOVERY_INCOMPLETE" }] } }, risk: { limits: [] } };
  if (path.startsWith("/api/candidates?")) return { items: emptyUniverse || filteredEmpty ? [] : [candidate], page: path.includes("page=2") ? 2 : 1, page_size: 50, total: emptyUniverse ? 0 : filteredEmpty ? 1 : 51, pages: 2 };
  if (path.startsWith(`/api/candidates/${candidate.wallet}`)) return { identity: { wallet: candidate.wallet, operator_state: "shadow", research_state: "qualified" }, score: { total: 87.3, eligible: true, components: { consistency: 9 }, penalties: { drawdown: 1 }, reasons: ["fixture_reason"] }, phase_a_prefilter_reasons: ["phase_a_fixture"], phase_b_hard_gates: ["phase_b_fixture"], target_performance: {}, follower_performance: {}, latency: { status: "unavailable" }, analysis_window: {} };
  return { items: [] };
}

function configureReadyLaneIIIUiFixture() {
  const sessionId = "MNQU6:ASIA_GLOBEX:2026-08-26";
  ledgerVerificationResponse = {
    status: "PASS", verification_id: "lv-current", verified_through_sequence: 100,
    chain_valid: true, checkpoint_valid: true, full_scan_required: false,
  };
  laneIIIPaperOverrides = {
    current_session_id: sessionId, session_generation: 7,
    current_position: "FLAT", current_quantity: 0, working_owned_orders: 0,
    entry_owner: "NONE", live_capital: "DENIED", commissioning_lifecycle: { active: false },
    market_observer: { market_observer_state: "ACTIVE" },
    commissioning_warmup: { status: "WARMED" },
    continuity: { healthy: true, local_bridge_healthy: true, market_price_connected: true },
    market_freshness: {
      quote: { fresh: true }, classified_trade: { fresh: true }, depth_mutation: { fresh: true },
    },
    transport: {
      state: "AUTHENTICATED", authenticated_client: true, reconciled: true,
      addon_provenance: { status: "MATCH" },
    },
  };
  laneIIILedgerOverrides = {
    highest_sequence: 125, verified_through_sequence: 100, unverified_tail_rows: 25,
    deferred_capacity: {
      schema: "l3g-ledger-writer-capacity-v1", state: "HEALTHY", admission_open: true,
      capacity_fault_latched: false, negative_headroom_sustained: false,
      writer_error: null, queue_growth_records_per_second: 0,
    },
    commissioning_ledger_state: "VERIFIED_ANCHOR_WITH_ACCEPTED_LIVE_TAIL",
    last_authority_mutation_sequence: 99, last_authority_mutation_domain: "SESSION",
    last_authority_mutation_kind: "SESSION_AUTHORITY", last_unknown_sequence: 0,
    last_unknown_domain: null, last_unknown_kind: null, last_blocking_sequence: 99,
    last_blocking_classification: "AUTHORITY_MUTATION", last_blocking_domain: "SESSION",
    last_blocking_kind: "SESSION_AUTHORITY",
  };
  commissioningRehearsalResponse = {
    result: "READY", generated_at: new Date().toISOString(), blocking_reasons: [],
    session: { session_id: sessionId, session_generation: 7 },
    ledger: {
      verification_id: "lv-current", verified_through_sequence: 100,
      arm_snapshot_tip: 125, unverified_tail_rows: 25,
      ledger_trust_state: "VERIFIED_ANCHOR_WITH_ACCEPTED_LIVE_TAIL",
      last_authority_mutation_sequence: 99, last_unknown_sequence: 0,
    },
  };
}

beforeEach(() => {
  localStorage.clear();
  WebSocketStub.instances = [];
  closeAllResponse = null;
  emptyUniverse = false;
  filteredEmpty = false;
  discoveryJobResponse = null;
  discoveryJobDetailResponse = null;
  shadowResponse = { configured: false, state: "NOT_CONFIGURED", freshness: "UNKNOWN" };
  ledgerVerificationResponse = { status: "UNVERIFIED", full_scan_required: true, chain_valid: false, checkpoint_valid: false, errors: [{ code: "NO_VERIFICATION_ARTIFACT", message: "No completed local ledger verification exists." }] };
  ledgerVerificationSchedule = { enabled: false, frequency: "DISABLED", local_time: "03:00", weekday: 0, mode: "auto" };
  laneIIIPaperOverrides = {};
  laneIIILedgerOverrides = {};
  commissioningRehearsalResponse = null;
  commissioningRehearsalSideEffect = null;
  commissioningStartResponse = null;
  commissioningStartSideEffect = null;
  operationalStartResponse = null;
  operationalStartSideEffect = null;
  slimStatusResponse = {
    generated_at: new Date().toISOString(), light: "RED", label: "NOT READY",
    message: "Paper runtime status is unavailable.", can_start: false, paper_active: false,
    ledger_verification: { state: "UNAVAILABLE", message: "Verification status is unavailable." },
    pnl: { state: "MISSING", total: null, realized: null, unrealized: null },
  };
  accountBalancesResponse = {
    accounts: {
      Sim101: { cash_value: 100123.45, cash_value_observed_at: "2026-08-29T15:00:00Z" },
      Lucid25kflex01: { cash_value: 24987.65, cash_value_observed_at: "2026-08-29T15:00:00Z" },
    },
  };
  laneIIILiveResponse = {
    terminal_status: "BLOCKED_LIVE_ACCOUNT_IDENTITY", state: "AUTHORIZATION_BOUNDARY_IMPLEMENTED",
    mechanical_commissioning: "COMMISSIONED", live_account_identity: "UNVERIFIED",
    authorization_boundary: "IMPLEMENTED", live_authority: "DISARMED", live_canary: "NOT_RUN",
    account_class: "UNKNOWN", authorized_account: null, live_capital: "DENIED",
    contract: "MNQ SEP26", maximum_quantity: 1, preflight_age_seconds: null,
    authorization_expires_at: null, gateway: "AUTHENTICATED_LOOPBACK", addon_provenance: "PASS",
    reconciliation: "PASS", protection: "PASS", kill_paths: "PASS", quarantine: false, locked: false,
    live_send_count: 0, one_control_start: { enabled: false, reason: "LIVE_ACCOUNT_IDENTITY_UNVERIFIED" },
    components: { LIVE_AUTHORITY: { state: "RED", reason: "DISARMED" } },
  };
  vi.stubGlobal("WebSocket", WebSocketStub);
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => new Response(JSON.stringify(payload(String(input))), { status: 200, headers: { "Content-Type": "application/json" } })));
});

afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

describe("copy control center", () => {
  it("separates L3H mechanical commissioning from live authorization and canary state", async () => {
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Live" }));
    expect(await screen.findByText("BLOCKED_LIVE_ACCOUNT_IDENTITY")).toBeInTheDocument();
    const panel = screen.getByRole("heading", { name: "Authorization boundary" }).closest("section") as HTMLElement;
    expect(panel).toHaveTextContent("COMMISSIONED");
    expect(panel).toHaveTextContent("IMPLEMENTED");
    expect(panel).toHaveTextContent("DISARMED");
    expect(panel).toHaveTextContent("NOT_RUN");
    expect(screen.getByRole("button", { name: "START LIVE — 1 MNQ CANARY" })).toBeDisabled();
  });

  it("does not let a sim account or server-provided enabled flag manufacture browser authority", async () => {
    laneIIILiveResponse = {
      ...laneIIILiveResponse, account_class: "LOCAL_SIMULATION", live_account_identity: "VERIFIED",
      live_authority: "ONE_SHOT_AUTHORIZED", authorized_account: "Sim101",
      one_control_start: { enabled: true, reason: "UNTRUSTED_FIXTURE" },
    };
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Live" }));
    const panel = await screen.findByRole("heading", { name: "Authorization boundary" });
    expect(panel.closest("section")).toHaveTextContent("UNVERIFIED");
    expect(panel.closest("section")).toHaveTextContent("DISARMED");
    expect(panel.closest("section")).not.toHaveTextContent("ONE_SHOT_AUTHORIZED");
    expect(screen.getByRole("button", { name: "START LIVE — 1 MNQ CANARY" })).toBeDisabled();
    expect(fetch).not.toHaveBeenCalledWith(expect.stringContaining("authorize"), expect.anything());
  });

  it("lets quarantine dominate a server-reported one-shot authorization", async () => {
    laneIIILiveResponse = {
      ...laneIIILiveResponse, account_class: "LIVE_CAPITAL", live_account_identity: "VERIFIED",
      live_authority: "ONE_SHOT_AUTHORIZED", authorized_account: "LIVE-SAFE-ID", quarantine: true,
    };
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Live" }));
    const panel = (await screen.findByRole("heading", { name: "Authorization boundary" })).closest("section") as HTMLElement;
    expect(panel).toHaveTextContent("VERIFIED");
    expect(panel).toHaveTextContent("DISARMED");
    expect(panel).not.toHaveTextContent("ONE_SHOT_AUTHORIZED");
    expect(screen.getByRole("button", { name: "START LIVE — 1 MNQ CANARY" })).toBeDisabled();
  });

  it("renders an expired capability as expired and disarmed", async () => {
    laneIIILiveResponse = {
      ...laneIIILiveResponse, account_class: "LIVE_CAPITAL", live_account_identity: "VERIFIED",
      live_authority: "ONE_SHOT_AUTHORIZED", authorized_account: "LIVE-SAFE-ID",
      authorization_expires_at: "2000-01-01T00:00:00Z",
    };
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Live" }));
    const panel = (await screen.findByRole("heading", { name: "Authorization boundary" })).closest("section") as HTMLElement;
    expect(panel).toHaveTextContent("EXPIRED");
    expect(panel).toHaveTextContent("DISARMED");
    expect(panel).not.toHaveTextContent("ONE_SHOT_AUTHORIZED");
  });

  it("ignores browser-restored authority state", async () => {
    localStorage.setItem("live_authority", "ONE_SHOT_AUTHORIZED");
    localStorage.setItem("account_class", "LIVE_CAPITAL");
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Live" }));
    const panel = (await screen.findByRole("heading", { name: "Authorization boundary" })).closest("section") as HTMLElement;
    expect(panel).toHaveTextContent("DISARMED");
    expect(screen.getByRole("button", { name: "START LIVE — 1 MNQ CANARY" })).toBeDisabled();
  });

  it("treats live-page selection as observational and performs no mutation", async () => {
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Live" }));
    await screen.findByText(/Viewing a live chart or this status is observational only/);
    const calls = vi.mocked(fetch).mock.calls;
    expect(calls.some(([, init]) => String(init?.method || "GET").toUpperCase() !== "GET")).toBe(false);
    expect(screen.getByRole("button", { name: "START LIVE — 1 MNQ CANARY" })).toBeDisabled();
  });

  it("fails closed when the live-status API is unavailable", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      if (String(input).startsWith("/api/lane-iii/live")) throw new Error("status unavailable");
      return new Response(JSON.stringify(payload(String(input))), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Live" }));
    expect(await screen.findByText("status unavailable")).toBeInTheDocument();
    const panel = screen.getByRole("heading", { name: "Authorization boundary" }).closest("section") as HTMLElement;
    expect(panel).toHaveTextContent("DISARMED");
    expect(panel).toHaveTextContent("NOT_RUN");
    expect(screen.getByRole("button", { name: "START LIVE — 1 MNQ CANARY" })).toBeDisabled();
  });

  it("shows observed Sim101 and LucidFlex balances instead of the seed paper equity", async () => {
    render(<App />);
    expect((await screen.findAllByText("$100,123.45")).length).toBeGreaterThan(0);
    expect(screen.getAllByText("$24,987.65").length).toBeGreaterThan(0);
    expect(screen.queryByText("Paper equity")).not.toBeInTheDocument();
  });

  it("renders paper-only controls and candidate table filtering/sorting surface", async () => {
    render(<App />);
    expect(await screen.findByText("PAPER POSITIONS ONLY")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Candidates" }));
    expect(await screen.findByText("0x111111…111111")).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Search wallet"), { target: { value: "0x111" } });
    fireEvent.click(screen.getByRole("button", { name: /Score/ }));
    await waitFor(() => expect(fetch).toHaveBeenCalledWith(expect.stringContaining("sort=score"), expect.anything()));
  });

  it("opens a clear PAPER close-all confirmation", async () => {
    render(<App />);
    expect(await screen.findByRole("button", { name: "Close All Paper Positions" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Close All Paper Positions" }));
    expect(screen.getByRole("dialog")).toHaveTextContent("flatten every current paper sleeve");
    expect(within(screen.getByRole("dialog")).getByRole("button", { name: "Close All Paper Positions" })).toBeInTheDocument();
  });

  it("renders canonical dossier score fields without legacy aliases", async () => {
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Candidates" }));
    fireEvent.click(await screen.findByText("0x111111…111111"));
    expect(await screen.findByText("RESEARCH DOSSIER")).toBeInTheDocument();
    expect(screen.getByText("consistency")).toBeInTheDocument();
    expect(screen.getByText("drawdown penalty")).toBeInTheDocument();
    expect(screen.getByText("fixture_reason")).toBeInTheDocument();
    expect(screen.getByText("Phase A prefilter reasons")).toBeInTheDocument();
    expect(screen.getByText("phase_a_fixture")).toBeInTheDocument();
    expect(screen.getByText("Phase B hard-gate failures")).toBeInTheDocument();
    expect(screen.getByText("phase_b_fixture")).toBeInTheDocument();
  });

  it("does not offer Active for a dossier without a selected Phase B finalist recommendation", async () => {
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Candidates" }));
    fireEvent.click(await screen.findByText("0x111111…111111"));
    expect(await screen.findByText("Activation is available only for a current Phase B selected finalist.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Activate PAPER" })).not.toBeInTheDocument();
  });

  it("shows the actual number of PAPER positions left open after a partial close-all", async () => {
    closeAllResponse = {
      status: "partial", closed: [{ wallet: candidate.wallet, symbol: "BTC" }], failed: [{ wallet: candidate.wallet, symbol: "ETH" }],
      remaining_open_positions: [{ sleeve_id: "sleeve-2", wallet: candidate.wallet, symbol: "ETH" }],
      control: { state: "PAUSED", entries_allowed: false, paper_only: true },
    };
    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Close All Paper Positions" }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Close All Paper Positions" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("1 PAPER position remains open");
  });

  it("renders watcher membership health received over the websocket", async () => {
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "System" }));
    await act(async () => {
      WebSocketStub.instances[0].onmessage?.({ data: JSON.stringify({ type: "watcher_health", data: { state: "CONNECTED", supervisor_state: "CONNECTED", desired_target_count: 2, subscribed_target_count: 2, membership_in_sync: true, active_entry_target_count: 1, open_sleeve_wallet_count: 1 } }) } as MessageEvent);
    });
    const panel = (await screen.findByText("System health")).closest("section");
    expect(panel).toHaveTextContent("Desired targets");
    expect(panel).toHaveTextContent("IN SYNC");
    expect(panel).toHaveTextContent("Open sleeve wallets");
  });

  it("renders Lane III ledger health without triggering a full scan", async () => {
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Paper" }));
    const panel = (await screen.findByText("Ledger health")).closest("section");
    const valueFor = (label: string) => within(panel as HTMLElement).getByText(label).nextElementSibling?.textContent;
    expect(panel).toHaveTextContent("L3G-PAPER-EPOCH-002");
    expect(panel).toHaveTextContent("VALID");
    expect(valueFor("Highest sequence")).toBe("125");
    expect(valueFor("Verified anchor")).toBe("100");
    expect(valueFor("Tail snapshot tip")).toBe("125");
    expect(valueFor("Tail rows")).toBe("25 rows");
    expect(valueFor("Tail trust state")).toBe("UNVERIFIED_AUTHORITY_TAIL");
    expect(valueFor("Last authority mutation sequence")).toBe("120");
    expect(valueFor("Last authority mutation domain")).toBe("RISK_EVENT");
    expect(valueFor("Last authority mutation kind")).toBe("RISK_LOCKOUT");
    expect(valueFor("Last authority observation sequence")).toBe("124");
    expect(valueFor("Last authority observation domain")).toBe("POSITION_SNAPSHOT");
    expect(valueFor("Last authority observation kind")).toBe("BROKER_STATE_OBSERVED");
    expect(valueFor("Last unknown sequence")).toBe("125");
    expect(valueFor("Last unknown domain")).toBe("FUTURE_DOMAIN");
    expect(valueFor("Last unknown kind")).toBe("FUTURE_KIND");
    expect(valueFor("Latest blocking classification")).toBe("UNKNOWN");
    expect(valueFor("Latest blocking sequence")).toBe("125");
    expect(valueFor("Latest blocking domain")).toBe("FUTURE_DOMAIN");
    expect(valueFor("Latest blocking kind")).toBe("FUTURE_KIND");
    expect(screen.queryByText(/DO_NOT_RENDER_TAIL_PAYLOAD/)).not.toBeInTheDocument();
  });

  it("enables atomic start only for a fresh rehearsal that still matches live readiness", async () => {
    configureReadyLaneIIIUiFixture();
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Paper" }));
    fireEvent.click(await screen.findByRole("button", { name: "Run Read-Only Commissioning Rehearsal" }));
    expect(await screen.findByText("READY FOR COMMISSIONING")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Atomic Commissioning Start" })).toBeEnabled();
  });

  it("invalidates a READY rehearsal when live warmup or freshness is lost", async () => {
    configureReadyLaneIIIUiFixture();
    commissioningRehearsalSideEffect = () => {
      laneIIIPaperOverrides = {
        ...laneIIIPaperOverrides,
        commissioning_warmup: { status: "NOT_WARMED" },
        market_freshness: {
          quote: { fresh: false }, classified_trade: { fresh: true }, depth_mutation: { fresh: true },
        },
      };
    };
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Paper" }));
    fireEvent.click(await screen.findByRole("button", { name: "Run Read-Only Commissioning Rehearsal" }));
    expect(await screen.findByText("REHEARSAL STALE")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Atomic Commissioning Start" })).toBeDisabled();
  });

  it("keeps an otherwise ready rehearsal blocked when writer capacity is no longer healthy", async () => {
    configureReadyLaneIIIUiFixture();
    commissioningRehearsalSideEffect = () => {
      laneIIILedgerOverrides = {
        ...laneIIILedgerOverrides,
        deferred_capacity: {
          schema: "l3g-ledger-writer-capacity-v1", state: "DEGRADED", admission_open: true,
          capacity_fault_latched: false, negative_headroom_sustained: true,
          writer_error: null, queue_growth_records_per_second: 7.5,
        },
      };
    };
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Paper" }));
    fireEvent.click(await screen.findByRole("button", { name: "Run Read-Only Commissioning Rehearsal" }));
    expect(await screen.findByText("REHEARSAL STALE")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Atomic Commissioning Start" })).toBeDisabled();
  });

  it("supersedes a READY rehearsal with newer live ledger blockers", async () => {
    configureReadyLaneIIIUiFixture();
    commissioningRehearsalSideEffect = () => {
      laneIIILedgerOverrides = {
        ...laneIIILedgerOverrides,
        highest_sequence: 126, unverified_tail_rows: 26,
        commissioning_ledger_state: "UNVERIFIED_UNKNOWN_TAIL",
        last_unknown_sequence: 126, last_unknown_domain: "FUTURE_DOMAIN",
        last_unknown_kind: "FUTURE_KIND", last_blocking_sequence: 126,
        last_blocking_classification: "UNKNOWN", last_blocking_domain: "FUTURE_DOMAIN",
        last_blocking_kind: "FUTURE_KIND",
      };
    };

    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Paper" }));
    fireEvent.click(await screen.findByRole("button", { name: "Run Read-Only Commissioning Rehearsal" }));
    expect(await screen.findByText("REHEARSAL STALE")).toBeInTheDocument();
    const panel = screen.getByText("Ledger health").closest("section") as HTMLElement;
    const valueFor = (label: string) => within(panel).getByText(label).nextElementSibling?.textContent;
    expect(valueFor("Highest sequence")).toBe("126");
    expect(valueFor("Tail trust state")).toBe("UNVERIFIED_UNKNOWN_TAIL");
    expect(valueFor("Latest blocking sequence")).toBe("126");
    expect(valueFor("Latest blocking kind")).toBe("FUTURE_KIND");
    expect(screen.getByRole("button", { name: "Atomic Commissioning Start" })).toBeDisabled();
  });

  it("starts a local ledger verifier and persists the local schedule controls", async () => {
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Paper" }));
    expect(await screen.findByText("Ledger Verification")).toBeInTheDocument();
    expect(screen.getByText("FULL VERIFICATION REQUIRED")).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Ledger verification mode"), { target: { value: "full" } });
    fireEvent.click(screen.getByRole("button", { name: "Verify Ledger Now" }));
    await waitFor(() => expect(fetch).toHaveBeenCalledWith("/api/lane-iii/paper/ledger-verification", expect.objectContaining({ method: "POST", body: JSON.stringify({ mode: "full" }) })));
    fireEvent.click(screen.getByLabelText("Enable ledger verification schedule"));
    fireEvent.change(screen.getByLabelText("Ledger verification frequency"), { target: { value: "WEEKLY" } });
    fireEvent.change(screen.getByLabelText("Scheduled ledger verification mode"), { target: { value: "incremental" } });
    fireEvent.click(screen.getByRole("button", { name: "Save Schedule" }));
    await waitFor(() => expect(fetch).toHaveBeenCalledWith("/api/lane-iii/paper/ledger-verification/schedule", expect.objectContaining({ method: "POST" })));
  });

  it("makes Discovery the clear fresh-install starting point with bounded presets", async () => {
    emptyUniverse = true;
    render(<App />);
    expect(await screen.findByText("Candidate universe not initialized")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Start Discovery" }));
    expect(await screen.findByText("Candidate Source")).toBeInTheDocument();
    expect(screen.getByText("QUICK SCAN")).toBeInTheDocument();
    expect(screen.getByText("STANDARD SCAN")).toBeInTheDocument();
    expect(screen.getByText("DEEP SCAN")).toBeInTheDocument();
    expect(screen.getByText("No usable AWS credentials were detected on this machine. No credentials are stored by Trader.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Start Candidate Discovery" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "Test Source Access" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Start Candidate Discovery" })).toBeEnabled());
    fireEvent.click(screen.getByRole("button", { name: "Start Candidate Discovery" }));
    expect(screen.getByRole("dialog")).toHaveTextContent("will not place trades");
  });

  it("exposes persistent selection semantics only for retained UI choices", async () => {
    render(<App />);
    const overview = screen.getByRole("button", { name: "Overview" });
    expect(overview).toHaveClass("selected");
    expect(overview).toHaveAttribute("aria-current", "page");
    fireEvent.click(screen.getByRole("button", { name: "Discovery" }));
    expect(screen.getByRole("button", { name: "Discovery" })).toHaveAttribute("aria-current", "page");
    expect(overview).not.toHaveAttribute("aria-current");

    await screen.findByText("Candidate Source");
    const quick = screen.getByRole("button", { name: /QUICK SCAN/ });
    const standard = screen.getByRole("button", { name: /STANDARD SCAN/ });
    const deep = screen.getByRole("button", { name: /DEEP SCAN/ });
    expect(standard).toHaveAttribute("aria-pressed", "true");
    expect(quick).toHaveAttribute("aria-pressed", "false");
    fireEvent.click(quick);
    expect(quick).toHaveAttribute("aria-pressed", "true");
    expect(standard).toHaveAttribute("aria-pressed", "false");
    fireEvent.click(deep);
    expect(deep).toHaveAttribute("aria-pressed", "true");
    expect(quick).toHaveAttribute("aria-pressed", "false");

    const start = screen.getByRole("button", { name: "Start Candidate Discovery" });
    expect(start).toBeDisabled();
    expect(start).not.toHaveAttribute("aria-pressed");
  });

  it("supersedes a locally queued discovery with the polled persisted failure", async () => {
    discoveryJobDetailResponse = {
      job_id: "discovery-1", status: "failed", stage: "discovery", progress_current: 1, progress_total: 1,
      message: "Unsupported node_fills_by_block event: expected a fill object.",
      error: { message: "Unsupported node_fills_by_block event: expected a fill object." },
      configuration: { preset: "quick", candidate_limit: 1000 }, result: { source_plan: { bytes_total: 32246406 } },
    };
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Discovery" }));
    await screen.findByText("Candidate Source");
    fireEvent.click(screen.getByRole("button", { name: "Test Source Access" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Start Candidate Discovery" })).toBeEnabled());
    fireEvent.click(screen.getByRole("button", { name: "Start Candidate Discovery" }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Start Candidate Discovery" }));
    expect(await screen.findByText("Discovery failed")).toBeInTheDocument();
    expect(screen.getAllByText("Unsupported node_fills_by_block event: expected a fill object.").length).toBeGreaterThan(0);
    expect(screen.queryByRole("button", { name: "Cancel Discovery" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry Discovery" })).toBeInTheDocument();
  });

  it("shows Hyperliquid REST rate-governance telemetry in System", async () => {
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "System" }));
    expect(await screen.findByText("Hyperliquid API")).toBeInTheDocument();
    expect(screen.getByText("REST budget")).toBeInTheDocument();
    expect(screen.getByText("429 retry signals")).toBeInTheDocument();
    expect(screen.getByText("Recovery gaps")).toBeInTheDocument();
  });

  it("exposes the read-only real-venue shadow state without live execution controls", async () => {
    shadowResponse = { configured: true, venue: "hyperliquid", account_id: candidate.wallet, state: "INCOMPLETE", freshness: "STALE", latest_observation: { comparison: { positions: { state: "INCOMPLETE" }, open_orders: { state: "INCOMPLETE" } } } };
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "System" }));
    expect(await screen.findByText("Real-venue shadow observation")).toBeInTheDocument();
    expect(screen.getByText("Public-account reads only. This cannot sign, submit, cancel, or enable live execution.")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Refresh read-only shadow observation" }));
    await waitFor(() => expect(fetch).toHaveBeenCalledWith("/api/execution/shadow/refresh", expect.objectContaining({ method: "POST" })));
    expect(screen.queryByRole("button", { name: /Enable live|Cancel order|Submit order/i })).not.toBeInTheDocument();
  });

  it("marks prior shadow evidence pending and coalesces repeated refresh clicks", async () => {
    shadowResponse = { configured: true, venue: "hyperliquid", account_id: candidate.wallet, state: "COMPLETE", freshness: "FRESH" };
    let resolveRefresh: (response: Response) => void = () => { throw new Error("Refresh promise was not created."); };
    const deferredFetch = vi.fn((input: RequestInfo | URL) => {
      if (String(input).startsWith("/api/execution/shadow/refresh")) {
        return new Promise<Response>((resolve) => { resolveRefresh = resolve; });
      }
      return Promise.resolve(new Response(JSON.stringify(payload(String(input))), { status: 200, headers: { "Content-Type": "application/json" } }));
    });
    vi.stubGlobal("fetch", deferredFetch);
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "System" }));
    const refresh = await screen.findByRole("button", { name: "Refresh read-only shadow observation" });
    fireEvent.click(refresh);
    expect(await screen.findByRole("status")).toHaveTextContent("Existing evidence is pending replacement");
    expect(refresh).toBeDisabled();
    fireEvent.click(refresh);
    expect(deferredFetch.mock.calls.filter(([path]) => String(path).startsWith("/api/execution/shadow/refresh"))).toHaveLength(1);
    resolveRefresh(new Response(JSON.stringify({ ...shadowResponse, state: "INCOMPLETE", freshness: "UNKNOWN" }), { status: 200, headers: { "Content-Type": "application/json" } }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Refresh read-only shadow observation" })).toBeEnabled());
    expect(screen.getByText("INCOMPLETE")).toBeInTheDocument();
  });

  it("shows discovery websocket progress and completion navigation", async () => {
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Discovery" }));
    await act(async () => {
      WebSocketStub.instances[0].onmessage?.({ data: JSON.stringify({ type: "discovery_job_update", data: { job_id: "discovery-1", status: "discovering", stage: "parsing", progress_current: 4, progress_total: 6, configuration: { preset: "standard", candidate_limit: 2500 }, result: { wallets_observed: 18432, eligible_wallets: 2714 } } }) } as MessageEvent);
    });
    expect(await screen.findByText("4 / 6 source objects")).toBeInTheDocument();
    expect(screen.getByText("18,432")).toBeInTheDocument();
    await act(async () => {
      WebSocketStub.instances[0].onmessage?.({ data: JSON.stringify({ type: "discovery_job_update", data: { job_id: "discovery-1", status: "completed", stage: "completed", configuration: { preset: "standard", candidate_limit: 2500 }, result: { registered_candidates: 2500 } } }) } as MessageEvent);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Open Candidates" }));
    expect(await screen.findByRole("heading", { name: "Candidates" })).toBeInTheDocument();
  });

  it("distinguishes an uninitialized candidate universe from a filtered empty result", async () => {
    emptyUniverse = true;
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Candidates" }));
    expect(await screen.findByText("No candidate universe exists yet.")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Open Discovery" }));
    expect(await screen.findByRole("heading", { name: "Discovery" })).toBeInTheDocument();
    cleanup();
    emptyUniverse = false; filteredEmpty = true;
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Candidates" }));
    expect(await screen.findByText("No candidates match these filters.")).toBeInTheDocument();
  });

  it("uses server pagination and applies live position websocket updates", async () => {
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Candidates" }));
    const next = await screen.findByRole("button", { name: "Next" });
    await waitFor(() => expect(next).not.toBeDisabled());
    fireEvent.click(next);
    await waitFor(() => expect(fetch).toHaveBeenCalledWith(expect.stringContaining("page=2"), expect.anything()));

    fireEvent.click(screen.getByRole("button", { name: "Positions" }));
    await act(async () => {
      WebSocketStub.instances[0].onmessage?.({ data: JSON.stringify({ type: "position_update", data: { items: [{ sleeve_id: "sleeve-1", target_wallet: candidate.wallet, symbol: "BTC", direction: "long", quantity: 1, entry_price: 100, current_mark: 101, allocated_capital: 10, remaining_capital: 10, allocation_bucket: "10%", unrealized_pnl: 1, realized_pnl: 0, fees: 0, opened_at: "2026-01-01T00:00:00Z", mark_fresh: true }] } }) } as MessageEvent);
    });
    expect(await screen.findByText("10%")).toBeInTheDocument();
  });

  it("renders the compact green Slim Mode with all three lights and the persistent paper-start path", async () => {
    slimStatusResponse = {
      generated_at: new Date().toISOString(), light: "GREEN", label: "READY TO START PAPER TRADING",
      message: "All canonical paper-start gates are currently satisfied.", can_start: true, paper_active: false,
      ledger_verification: { state: "PASS", completed_at: "2026-09-01T14:00:00Z", message: "Verified" },
      pnl: { state: "CURRENT", total: "10.25", realized: "12.50", unrealized: "-2.25" },
    };
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Slim Console" }));
    expect(await screen.findByRole("img", { name: "Readiness: GREEN" })).toBeInTheDocument();
    expect(document.querySelectorAll(".slim-light")).toHaveLength(3);
    const start = screen.getByRole("button", { name: "Start Paper Trading" });
    expect(start).toBeEnabled();
    fireEvent.click(start);
    await waitFor(() => expect(fetch).toHaveBeenCalledWith("/api/lane-iii/paper/operational-start", expect.objectContaining({ method: "POST" })));
    expect(fetch).not.toHaveBeenCalledWith("/api/lane-iii/paper/commissioning-start", expect.anything());
    expect(screen.getByText("$10.25")).toBeInTheDocument();
  });

  it("fails closed when canonical state changes after Slim rendered green", async () => {
    slimStatusResponse = {
      generated_at: new Date().toISOString(), light: "GREEN", label: "READY TO START PAPER TRADING",
      message: "All canonical paper-start gates are currently satisfied.", can_start: true, paper_active: false,
      ledger_verification: { state: "PASS", completed_at: "2026-09-01T14:00:00Z", message: "Verified" },
      pnl: { state: "CURRENT", total: "1", realized: "1", unrealized: "0" },
    };
    operationalStartResponse = { started: false, armed: false, state: "READY_DISARMED", reason_codes: ["OPERATIONAL_PAPER_PREFLIGHT_STALE"] };
    operationalStartSideEffect = () => {
      slimStatusResponse = {
        generated_at: new Date().toISOString(), light: "RED", label: "NOT READY",
        message: "Runtime changed during the last readiness check.", can_start: false, paper_active: false,
        ledger_verification: { state: "PASS", completed_at: "2026-09-01T14:00:00Z", message: "Verified" },
        pnl: { state: "CURRENT", total: "1", realized: "1", unrealized: "0" },
      };
    };
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Slim Console" }));
    fireEvent.click(await screen.findByRole("button", { name: "Start Paper Trading" }));
    expect(await screen.findByRole("img", { name: "Readiness: RED" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Start Paper Trading" })).toBeDisabled();
  });

  it("fails closed in red or yellow Slim states, protects duplicate verification, and never renders stale P&L as zero", async () => {
    slimStatusResponse = {
      generated_at: new Date().toISOString(), light: "YELLOW", label: "PREPARING",
      message: "Ledger verification is still running.", can_start: false, paper_active: false,
      ledger_verification: { state: "IN_PROGRESS", message: "Verifying…" },
      pnl: { state: "STALE", total: null, realized: null, unrealized: null },
    };
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Slim Console" }));
    expect(await screen.findByRole("img", { name: "Readiness: YELLOW" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Start Paper Trading" })).toBeDisabled();
    expect(screen.getByText("STALE")).toBeInTheDocument();
    expect(screen.queryByText("$0.00")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Ledger Verification" })).toBeDisabled();
  });

  it("preserves an active backend paper session across refresh and mode changes, then uses STOP TRADING", async () => {
    slimStatusResponse = {
      generated_at: new Date().toISOString(), light: "GREEN", label: "PAPER TRADING ACTIVE",
      message: "Sim101 paper operation is healthy and protected.", can_start: false, paper_active: true,
      ledger_verification: { state: "PASS", completed_at: "2026-09-01T14:00:00Z", message: "Verified" },
      pnl: { state: "CURRENT", total: "5", realized: "4", unrealized: "1" },
    };
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Slim Console" }));
    expect(localStorage.getItem("beezconsole-console-mode")).toBe("slim");
    fireEvent.click(screen.getByRole("button", { name: "Full Console" }));
    expect(await screen.findByRole("button", { name: "Lane III Paper" })).toBeInTheDocument();
    expect(localStorage.getItem("beezconsole-console-mode")).toBe("full");
    expect(fetch).not.toHaveBeenCalledWith("/api/lane-iii/paper/flatten-and-disarm", expect.anything());
    fireEvent.click(screen.getByRole("button", { name: "Slim Console" }));
    const stop = await screen.findByRole("button", { name: "STOP TRADING" });
    fireEvent.click(stop);
    await waitFor(() => expect(fetch).toHaveBeenCalledWith("/api/lane-iii/paper/flatten-and-disarm", expect.objectContaining({ method: "POST" })));
    expect(fetch).not.toHaveBeenCalledWith("/api/lane-iii/paper/operational-start", expect.anything());
  });

  it("falls back to Full Console for an invalid saved mode preference", async () => {
    localStorage.setItem("beezconsole-console-mode", "unsupported-mode");
    render(<App />);
    expect(await screen.findByRole("button", { name: "Lane III Paper" })).toBeInTheDocument();
  });

  it("keeps a requested Slim preference across refresh and coalesces duplicate verification clicks", async () => {
    slimStatusResponse = {
      generated_at: new Date().toISOString(), light: "RED", label: "NOT READY",
      message: "Run a current ledger verification.", can_start: false, paper_active: false,
      ledger_verification: { state: "UNVERIFIED", message: "Verification required" },
      pnl: { state: "MISSING", total: null, realized: null, unrealized: null },
    };
    let resolveVerification: (response: Response) => void = () => { throw new Error("Verification promise was not created."); };
    const deferredFetch = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).startsWith("/api/lane-iii/paper/ledger-verification") && init?.method === "POST") {
        return new Promise<Response>((resolve) => { resolveVerification = resolve; });
      }
      return Promise.resolve(new Response(JSON.stringify(payload(String(input))), { status: 200, headers: { "Content-Type": "application/json" } }));
    });
    vi.stubGlobal("fetch", deferredFetch);
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Slim Console" }));
    const verify = await screen.findByRole("button", { name: "Ledger Verification" });
    fireEvent.click(verify);
    expect(await screen.findByText("Verifying…")).toBeInTheDocument();
    expect(verify).toBeDisabled();
    fireEvent.click(verify);
    expect(deferredFetch.mock.calls.filter(([path, init]) => String(path).startsWith("/api/lane-iii/paper/ledger-verification") && (init as RequestInit | undefined)?.method === "POST")).toHaveLength(1);
    resolveVerification(new Response(JSON.stringify({ status: "IN_PROGRESS" }), { status: 200, headers: { "Content-Type": "application/json" } }));
    await waitFor(() => expect(deferredFetch.mock.calls.filter(([path]) => String(path).startsWith("/api/lane-iii/paper/slim-status"))).not.toHaveLength(0));
    cleanup();
    render(<App />);
    expect(await screen.findByRole("button", { name: "Full Console" })).toBeInTheDocument();
  });

  it("shows a concise HTML-endpoint failure in both Slim and Full Console", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path.startsWith("/api/lane-iii/paper/slim-status") || path === "/api/lane-iii/paper") {
        return new Response("<!doctype html><html></html>", { status: 200, headers: { "Content-Type": "text/html; charset=utf-8" } });
      }
      return new Response(JSON.stringify(payload(path)), { status: 200, headers: { "Content-Type": "application/json" } });
    }));

    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Slim Console" }));
    expect(await screen.findByText("Status unavailable — backend endpoint returned HTML")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "STOP TRADING" })).toBeEnabled();

    cleanup();
    localStorage.clear();
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Lane III Paper" }));
    expect(await screen.findByText("Paper status unavailable — backend endpoint returned HTML")).toBeInTheDocument();
  });
});
