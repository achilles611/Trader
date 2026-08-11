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

function payload(path: string) {
  if (path.startsWith("/api/overview")) return { counts: { total_discovered: emptyUniverse ? 0 : 20, qualified: 2, shadow: 1, active: 0 }, funnel: [], top_candidates: [candidate], recent_activity: [] };
  if (path.startsWith("/api/portfolio")) return { equity: 210, cash: 190, committed_capital: 10, open_pnl: 1, realized_pnl_total: 9, max_drawdown: 0.02, open_positions: 1 };
  if (path.startsWith("/api/controls/close-all-paper-positions")) return closeAllResponse || { status: "completed", control: { state: "RUNNING", entries_allowed: true, paper_only: true } };
  if (path.startsWith("/api/controls")) return { state: "RUNNING", entries_allowed: true, paper_only: true };
  if (path.startsWith("/api/discovery/status")) return { candidate_universe_count: emptyUniverse ? 0 : 20, source: { source: "Official HyperCore node data", connection_state: "SETUP REQUIRED", aws_credentials_detected: false, requester_pays_access: "not tested", message: "No usable AWS credentials were detected on this machine. No credentials are stored by Trader.", cache: { object_count: 0, size_bytes: 0 } }, presets: { quick: { window_hours: 1, candidate_limit: 1000, min_activity: 2, max_activity_age: "30d" }, standard: { window_hours: 6, candidate_limit: 2500, min_activity: 2, max_activity_age: "30d" }, deep: { window_hours: 24, candidate_limit: 5000, min_activity: 2, max_activity_age: "30d" } } };
  if (path.startsWith("/api/discovery/source/test")) return { source: "Official HyperCore node data", connection_state: "READY", aws_credentials_detected: true, requester_pays_access: "ready", cache: { object_count: 1, size_bytes: 42 } };
  if (path.startsWith("/api/discovery/jobs")) return discoveryJobResponse || { job_id: "discovery-1", status: "queued", stage: "queued", configuration: { preset: "standard", candidate_limit: 2500 } };
  if (path.startsWith("/api/system")) return { health: { mode: "paper", paper_only: true, database: { connected: true }, websocket: { available: true }, watcher: { state: "NOT_ATTACHED", desired_target_count: 0, subscribed_target_count: 0, membership_in_sync: true } }, risk: { limits: [] } };
  if (path.startsWith("/api/candidates?")) return { items: emptyUniverse || filteredEmpty ? [] : [candidate], page: path.includes("page=2") ? 2 : 1, page_size: 50, total: emptyUniverse ? 0 : filteredEmpty ? 1 : 51, pages: 2 };
  if (path.startsWith(`/api/candidates/${candidate.wallet}`)) return { identity: { wallet: candidate.wallet, operator_state: "shadow", research_state: "qualified" }, score: { total: 87.3, eligible: true, components: { consistency: 9 }, penalties: { drawdown: 1 }, reasons: ["fixture_reason"] }, target_performance: {}, follower_performance: {}, latency: { status: "unavailable" }, analysis_window: {} };
  return { items: [] };
}

beforeEach(() => {
  WebSocketStub.instances = [];
  closeAllResponse = null;
  emptyUniverse = false;
  filteredEmpty = false;
  discoveryJobResponse = null;
  vi.stubGlobal("WebSocket", WebSocketStub);
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => new Response(JSON.stringify(payload(String(input))), { status: 200, headers: { "Content-Type": "application/json" } })));
});

afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

describe("copy control center", () => {
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
});
