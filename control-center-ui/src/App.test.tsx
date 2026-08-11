import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";

class WebSocketStub {
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: (() => void) | null = null;
  close() {}
  constructor(_url: string) {}
}

const candidate = {
  wallet: "0x1111111111111111111111111111111111111111", operator_state: "shadow", research_state: "qualified",
  score: 87.3, qualified: true, stale_analysis: true, target_net_pnl: 12, follower_net_pnl: 9,
  win_rate: 0.62, profit_factor: 1.8, target_max_drawdown: 0.08, follower_max_drawdown: 0.1, coverage: "PROVEN_COMPLETE", source_count: 2,
};

function payload(path: string) {
  if (path.startsWith("/api/overview")) return { counts: { total_discovered: 20, qualified: 2, shadow: 1, active: 0 }, funnel: [], top_candidates: [candidate], recent_activity: [] };
  if (path.startsWith("/api/portfolio")) return { equity: 210, cash: 190, committed_capital: 10, open_pnl: 1, realized_pnl_total: 9, max_drawdown: 0.02, open_positions: 1 };
  if (path.startsWith("/api/controls")) return { state: "RUNNING", entries_allowed: true, paper_only: true };
  if (path.startsWith("/api/candidates?")) return { items: [candidate], page: 1, page_size: 50, total: 1, pages: 1 };
  if (path.startsWith(`/api/candidates/${candidate.wallet}`)) return { identity: { wallet: candidate.wallet, operator_state: "shadow", research_state: "qualified" }, score: { total: 87.3, eligible: true, component_scores: { consistency: 9 }, penalties: {}, reason_codes: [] }, target_performance: {}, follower_performance: {}, latency: { status: "unavailable" }, analysis_window: {} };
  return { items: [] };
}

beforeEach(() => {
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
});
