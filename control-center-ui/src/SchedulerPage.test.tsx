import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SchedulerPage } from "./SchedulerPage";

const catalog = {
  authority: "OBSERVE_VERIFY_NOTIFY_EXPORT_ONLY",
  tasks: [
    { task_type: "operator.reminder", display_name: "Operator reminder", description: "Durable reminder", domain: "Operations", authority_classification: "OPERATOR_NOTIFICATION" },
    { task_type: "lane_iii.session_readiness", display_name: "Lane III readiness", description: "Read-only", domain: "Lane III", authority_classification: "READ_ONLY" },
  ],
  templates: [{ template_id: "asia-readiness", name: "Asia readiness", task_type: "lane_iii.session_readiness" }],
};

function response(path: string) {
  if (path.startsWith("/api/scheduler/status")) return { state: "LEADER", database: { quick_check: "ok" }, timezone: "America/Denver", active_workers: 1, concurrency_limit: 2, unread_notifications: 1, counts: { SCHEDULED: 1, ACTIVE: 1, COMPLETE: 1, FAILED: 1 }, next_due: { due_at: "2030-01-01T00:00:00Z" } };
  if (path.startsWith("/api/scheduler/catalog")) return catalog;
  if (path.startsWith("/api/scheduler/schedules")) return { items: [{ schedule_id: "schedule-1", current_revision: 1, name: "Reminder", task_type: "operator.reminder", trigger_kind: "ONCE", timezone: "America/Denver", lifecycle: "ENABLED", next_due_at: "2030-01-01T00:00:00Z" }] };
  if (path.startsWith("/api/scheduler/runs")) return { items: [{ run_id: "run-active", status: "ACTIVE", task_type: "operator.reminder", schedule_id: "schedule-1", attempt: 1 }, { run_id: "run-complete", status: "COMPLETE", task_type: "operator.reminder", schedule_id: "schedule-1", attempt: 1 }] };
  if (path.startsWith("/api/scheduler/notifications")) return { items: [{ notification_id: "notice-1", severity: "warning", title: "Reminder", body: "Review gates" }] };
  if (path.startsWith("/api/scheduler/preview")) return { occurrences: [{ local_time: "2030-01-01T10:00:00-07:00", due_at: "2030-01-01T17:00:00Z" }] };
  return {};
}

beforeEach(() => {
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => new Response(JSON.stringify(response(String(input))), { status: 200, headers: { "Content-Type": "application/json" } })));
});
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

describe("Task Scheduler", () => {
  it("shows durable scheduler status, lifecycle filtering, templates, and no command surface", async () => {
    render(<SchedulerPage revision={0} notify={() => undefined} confirmation={() => undefined} />);
    expect(await screen.findByText("Operations scheduler")).toBeInTheDocument();
    expect(screen.getByText("LEADER")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Runs" }));
    fireEvent.click(screen.getByRole("button", { name: "Active" }));
    expect((await screen.findAllByText("ACTIVE")).length).toBeGreaterThan(1);
    expect(screen.getAllByText("operator.reminder")).toHaveLength(1);
    fireEvent.click(screen.getByRole("button", { name: "Schedules" }));
    expect(screen.getByText("Reminder")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Create paused" })).toBeInTheDocument();
    expect(screen.queryByLabelText(/command|script|executable/i)).not.toBeInTheDocument();
  });

  it("creates a backend-previewed reminder schedule and displays the session fail-closed rule", async () => {
    render(<SchedulerPage revision={0} notify={() => undefined} confirmation={() => undefined} />);
    fireEvent.click(await screen.findByRole("button", { name: "Create schedule" }));
    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "Morning reminder" } });
    fireEvent.change(screen.getByLabelText("Reminder title"), { target: { value: "Review" } });
    fireEvent.change(screen.getByLabelText("Message"), { target: { value: "Review readiness" } });
    fireEvent.change(screen.getByLabelText("Local date/time"), { target: { value: "2030-01-01T10:00" } });
    fireEvent.click(screen.getByRole("button", { name: "Preview next five" }));
    expect(await screen.findByText(/2030-01-01T17:00:00Z/)).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Trigger type"), { target: { value: "SESSION_RELATIVE" } });
    expect(await screen.findByText(/Session-relative triggers force SKIP/)).toBeInTheDocument();
    expect(screen.getByLabelText("Missed-run policy")).toBeDisabled();
    fireEvent.click(screen.getAllByRole("button", { name: "Create schedule" }).at(-1)!);
    await waitFor(() => expect(fetch).toHaveBeenCalledWith("/api/scheduler/schedules", expect.objectContaining({ method: "POST" })));
  });
});
