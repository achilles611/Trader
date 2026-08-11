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

function payload(path: string) {
  if (path.startsWith("/api/overview")) return { counts: { total_discovered: 20, qualified: 2, shadow: 1, active: 0 }, funnel: [], top_candidates: [candidate], recent_activity: [] };
  if (path.startsWith("/api/portfolio")) return { equity: 210, cash: 190, committed_capital: 10, open_pnl: 1, realized_pnl_total: 9, max_drawdown: 0.02, open_positions: 1 };
  if (path.startsWith("/api/controls")) return { state: "RUNNING", entries_allowed: true, paper_only: true };
  if (path.startsWith("/api/candidates?")) return { items: [candidate], page: path.includes("page=2") ? 2 : 1, page_size: 50, total: 51, pages: 2 };
  if (path.startsWith(`/api/candidates/${candidate.wallet}`)) return { identity: { wallet: candidate.wallet, operator_state: "shadow", research_state: "qualified" }, score: { total: 87.3, eligible: true, component_scores: { consistency: 9 }, penalties: {}, reason_codes: [] }, target_performance: {}, follower_performance: {}, latency: { status: "unavailable" }, analysis_window: {} };
  return { items: [] };
}

beforeEach(() => {
  WebSocketStub.instances = [];
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
