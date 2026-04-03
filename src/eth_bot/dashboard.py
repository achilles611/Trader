from __future__ import annotations

import os
from datetime import datetime
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .storage import save_json


DEFAULT_POLL_INTERVAL_MS = 3000


def _family_for_instance(instance_id: str) -> str:
    if instance_id.startswith("zerk"):
        return "zerk"
    if instance_id.startswith("tr"):
        return "tr"
    return "unknown"


def _dashboard_dir(root_dir: Path, generation: int) -> Path:
    return root_dir / "viz" / "dashboard" / f"generation_{generation:03d}"


def _relative_path(target: Path, start: Path) -> str:
    return Path(os.path.relpath(target, start)).as_posix()


def _build_manifest(root_dir: Path, generation: int, instance_ids: list[str]) -> dict[str, Any]:
    dashboard_dir = _dashboard_dir(root_dir, generation)
    generation_segment = f"generation_{generation:03d}"
    instances = []

    for instance_id in instance_ids:
        instances.append(
            {
                "instance_id": instance_id,
                "family": _family_for_instance(instance_id),
                "paths": {
                    "network_svg": _relative_path(root_dir / "viz" / "instances" / instance_id / "network.svg", dashboard_dir),
                    "network_json": _relative_path(
                        root_dir / "viz" / "instances" / instance_id / "network.json",
                        dashboard_dir,
                    ),
                    "activations_latest": _relative_path(
                        root_dir / "viz" / "instances" / instance_id / "activations_latest.json",
                        dashboard_dir,
                    ),
                    "session_report": _relative_path(
                        root_dir / "reports" / generation_segment / "instances" / instance_id / "session_report.json",
                        dashboard_dir,
                    ),
                },
            }
        )

    return {
        "generation": generation,
        "created_at": datetime.utcnow().isoformat(),
        "instance_ids": instance_ids,
        "instances": instances,
    }


def _render_dashboard_html(generation: int, poll_interval_ms: int = DEFAULT_POLL_INTERVAL_MS) -> str:
    template = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Trader Swarm Dashboard</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #101417;
      --panel: #171d22;
      --panel-2: #1d252b;
      --border: #2a3640;
      --text: #edf2f7;
      --muted: #91a3b4;
      --green: #37d67a;
      --green-soft: rgba(55, 214, 122, 0.14);
      --red: #ff6b6b;
      --red-soft: rgba(255, 107, 107, 0.14);
      --amber: #f6ad55;
      --cyan: #58c4dd;
      --shadow: 0 16px 40px rgba(0, 0, 0, 0.26);
      --radius: 18px;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: Consolas, "Liberation Mono", Menlo, monospace;
      background:
        radial-gradient(circle at top right, rgba(88, 196, 221, 0.08), transparent 25%),
        radial-gradient(circle at top left, rgba(55, 214, 122, 0.06), transparent 25%),
        var(--bg);
      color: var(--text);
    }

    .page {
      padding: 20px 24px 28px;
    }

    .topbar {
      display: grid;
      grid-template-columns: minmax(280px, 1.4fr) minmax(320px, 2fr);
      gap: 16px;
      margin-bottom: 18px;
    }

    .hero,
    .summary {
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.03), rgba(255, 255, 255, 0.01));
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 18px 20px;
    }

    .hero h1 {
      margin: 0 0 10px;
      font-size: 28px;
      letter-spacing: 0.03em;
    }

    .hero p,
    .footer-note {
      margin: 0;
      color: var(--muted);
      line-height: 1.5;
    }

    .hero-meta,
    .summary-grid,
    .card-status,
    .metric-grid,
    .stats-grid,
    .traits-grid,
    .links-row {
      display: grid;
      gap: 10px;
    }

    .hero-meta {
      grid-template-columns: repeat(3, minmax(0, 1fr));
      margin-top: 14px;
    }

    .summary-grid {
      grid-template-columns: repeat(5, minmax(0, 1fr));
      margin-top: 12px;
    }

    .meta-chip,
    .summary-card,
    .status-pill,
    .metric,
    .stat,
    .trait,
    .links-row a {
      border: 1px solid var(--border);
      border-radius: 14px;
      background: var(--panel-2);
    }

    .meta-chip,
    .summary-card {
      padding: 10px 12px;
    }

    .meta-chip strong,
    .summary-card strong,
    .metric strong,
    .stat strong,
    .trait strong {
      display: block;
      margin-top: 6px;
      font-size: 18px;
      color: var(--text);
    }

    .meta-chip span,
    .summary-card span,
    .metric span,
    .stat span,
    .trait span {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .swarm-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 16px;
    }

    .bot-card {
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.03), rgba(255, 255, 255, 0.01));
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 14px;
      min-height: 680px;
    }

    .card-header {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
    }

    .card-title {
      margin: 0;
      font-size: 24px;
    }

    .card-subtitle {
      margin-top: 6px;
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      line-height: 1.5;
    }

    .card-status {
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }

    .status-pill {
      padding: 8px 10px;
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      text-align: center;
    }

    .status-pill.live,
    .result-pill.win,
    .bar-fill.long {
      color: var(--green);
    }

    .status-pill.stale,
    .result-pill.flat {
      color: var(--amber);
    }

    .status-pill.halted,
    .result-pill.loss,
    .bar-fill.short {
      color: var(--red);
    }

    .metric-grid,
    .stats-grid,
    .traits-grid {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    .metric,
    .stat,
    .trait {
      padding: 10px 12px;
    }

    .bars {
      display: grid;
      gap: 10px;
    }

    .bar-row {
      display: grid;
      gap: 6px;
    }

    .bar-row label {
      display: flex;
      justify-content: space-between;
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }

    .bar-track {
      width: 100%;
      height: 12px;
      border-radius: 999px;
      background: #11161a;
      overflow: hidden;
      border: 1px solid rgba(255, 255, 255, 0.08);
    }

    .bar-fill {
      height: 100%;
      width: 0%;
      transition: width 0.25s ease;
      background:
        linear-gradient(90deg, rgba(255, 255, 255, 0.08), transparent),
        linear-gradient(90deg, var(--green), #86efac);
    }

    .bar-fill.short {
      background:
        linear-gradient(90deg, rgba(255, 255, 255, 0.08), transparent),
        linear-gradient(90deg, var(--red), #feb2b2);
    }

    .svg-wrap {
      border: 1px solid var(--border);
      border-radius: 14px;
      background: #f7f4ef;
      min-height: 210px;
      overflow: hidden;
      position: relative;
    }

    .svg-wrap .placeholder,
    .message {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }

    .svg-wrap .placeholder {
      position: absolute;
      inset: 0;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 18px;
      text-align: center;
      background: rgba(12, 16, 20, 0.72);
    }

    .svg-wrap svg,
    .svg-wrap img {
      width: 100%;
      height: auto;
      display: block;
    }

    .message {
      padding: 12px 14px;
      border: 1px dashed var(--border);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.02);
    }

    .result-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      padding: 4px 10px;
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid rgba(255, 255, 255, 0.08);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-right: 8px;
    }

    .links-row {
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }

    .links-row a {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 38px;
      color: var(--cyan);
      text-decoration: none;
      font-size: 12px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      padding: 8px 10px;
    }

    .footer-note {
      margin-top: 16px;
      text-align: center;
    }

    .positive {
      color: var(--green);
    }

    .negative {
      color: var(--red);
    }

    .muted {
      color: var(--muted);
    }

    @media (max-width: 1800px) {
      .swarm-grid {
        grid-template-columns: repeat(3, minmax(0, 1fr));
      }
    }

    @media (max-width: 1280px) {
      .topbar {
        grid-template-columns: 1fr;
      }

      .summary-grid {
        grid-template-columns: repeat(3, minmax(0, 1fr));
      }

      .swarm-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }

    @media (max-width: 760px) {
      .page {
        padding: 14px;
      }

      .hero-meta,
      .summary-grid,
      .card-status,
      .metric-grid,
      .stats-grid,
      .traits-grid,
      .links-row,
      .swarm-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="topbar">
      <div class="hero">
        <h1>Trader Swarm Dashboard</h1>
        <p>Generation __GENERATION_DISPLAY__ live wall of minds. The page polls local swarm artifacts and updates without a full reload.</p>
        <div class="hero-meta">
          <div class="meta-chip">
            <span>Generation</span>
            <strong id="generation-label">__GENERATION_DISPLAY__</strong>
          </div>
          <div class="meta-chip">
            <span>Last Refresh</span>
            <strong id="last-refresh">Waiting...</strong>
          </div>
          <div class="meta-chip">
            <span>Polling</span>
            <strong id="poll-interval">__POLL_MS__ ms</strong>
          </div>
        </div>
      </div>
      <div class="summary">
        <p>Swarm status summary</p>
        <div class="summary-grid">
          <div class="summary-card">
            <span>Instances Live</span>
            <strong id="summary-live">0 / 10</strong>
          </div>
          <div class="summary-card">
            <span>Total Equity</span>
            <strong id="summary-equity">$0.00</strong>
          </div>
          <div class="summary-card">
            <span>Open Positions</span>
            <strong id="summary-open">0</strong>
          </div>
          <div class="summary-card">
            <span>Halted</span>
            <strong id="summary-halted">0</strong>
          </div>
          <div class="summary-card">
            <span>Reports Ready</span>
            <strong id="summary-reports">0</strong>
          </div>
        </div>
      </div>
    </section>

    <section id="swarm-grid" class="swarm-grid"></section>
    <p class="footer-note">Live from local swarm artifacts. Polling interval: __POLL_MS__ ms.</p>
  </div>

  <script>
    const POLL_INTERVAL_MS = __POLL_MS__;
    const DASHBOARD_GENERATION = __GENERATION_VALUE__;
    const DASHBOARD_GENERATION_LABEL = "__GENERATION_DISPLAY__";
    const manifestUrl = "./dashboard_manifest.json";
    let manifest = null;

    const currencyFormatter = new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });

    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function cacheBust(url) {
      const separator = url.includes("?") ? "&" : "?";
      return `${url}${separator}t=${Date.now()}`;
    }

    async function safeFetchJson(url) {
      try {
        const response = await fetch(cacheBust(url), { cache: "no-store" });
        if (!response.ok) {
          return { ok: false, error: `HTTP ${response.status}` };
        }
        const text = await response.text();
        if (!text.trim()) {
          return { ok: false, error: "empty" };
        }
        return { ok: true, data: JSON.parse(text) };
      } catch (error) {
        return { ok: false, error: error.message || "fetch_failed" };
      }
    }

    async function safeFetchText(url) {
      try {
        const response = await fetch(cacheBust(url), { cache: "no-store" });
        if (!response.ok) {
          return { ok: false, error: `HTTP ${response.status}` };
        }
        const text = await response.text();
        if (!text.trim()) {
          return { ok: false, error: "empty" };
        }
        return { ok: true, data: text };
      } catch (error) {
        return { ok: false, error: error.message || "fetch_failed" };
      }
    }

    function formatMoney(value) {
      const number = Number(value);
      if (!Number.isFinite(number)) {
        return "n/a";
      }
      return currencyFormatter.format(number);
    }

    function formatPercent(value) {
      const number = Number(value);
      if (!Number.isFinite(number)) {
        return "n/a";
      }
      return `${number.toFixed(2)}%`;
    }

    function formatNumber(value, digits = 2) {
      const number = Number(value);
      if (!Number.isFinite(number)) {
        return "n/a";
      }
      return number.toFixed(digits);
    }

    function formatTimestamp(value) {
      if (!value) {
        return "Waiting...";
      }
      const parsed = new Date(value);
      if (Number.isNaN(parsed.getTime())) {
        return value;
      }
      return parsed.toLocaleString();
    }

    function dataFreshnessLabel(updatedAt) {
      if (!updatedAt) {
        return { text: "waiting", className: "stale" };
      }
      const ageMs = Date.now() - new Date(updatedAt).getTime();
      if (!Number.isFinite(ageMs)) {
        return { text: "unknown", className: "stale" };
      }
      if (ageMs > POLL_INTERVAL_MS * 3) {
        return { text: "stale", className: "stale" };
      }
      return { text: "live", className: "live" };
    }

    function buildCard(instance) {
      const card = document.createElement("article");
      card.className = "bot-card";
      card.dataset.instanceId = instance.instance_id;
      card.innerHTML = `
        <div class="card-header">
          <div>
            <h2 class="card-title">${escapeHtml(instance.instance_id)}</h2>
            <div class="card-subtitle">
              <span class="family-label">${escapeHtml(instance.family)}</span>
              <span> | </span>
              <span class="profile-label muted">waiting for first update...</span>
            </div>
          </div>
          <div class="status-pill stale freshness-pill">waiting</div>
        </div>

        <div class="card-status">
          <div class="status-pill market-pill">state: --</div>
          <div class="status-pill position-pill">position: flat</div>
          <div class="status-pill halt-pill">halted: no</div>
          <div class="status-pill generation-pill">gen: ${escapeHtml(DASHBOARD_GENERATION_LABEL)}</div>
        </div>

        <div class="metric-grid">
          <div class="metric">
            <span>Equity</span>
            <strong class="equity-value">n/a</strong>
          </div>
          <div class="metric">
            <span>Price</span>
            <strong class="price-value">n/a</strong>
          </div>
          <div class="metric">
            <span>Last Signal</span>
            <strong class="signal-value">waiting</strong>
          </div>
          <div class="metric">
            <span>Halt Reason</span>
            <strong class="halt-reason-value">none</strong>
          </div>
        </div>

        <div class="bars">
          <div class="bar-row">
            <label><span>Prob Win Long</span><span class="prob-long-label">n/a</span></label>
            <div class="bar-track"><div class="bar-fill long prob-long-bar"></div></div>
          </div>
          <div class="bar-row">
            <label><span>Prob Win Short</span><span class="prob-short-label">n/a</span></label>
            <div class="bar-track"><div class="bar-fill short prob-short-bar"></div></div>
          </div>
        </div>

        <div class="svg-wrap">
          <div class="placeholder svg-placeholder">waiting for first update...</div>
          <div class="svg-host"></div>
        </div>

        <div class="stats-grid">
          <div class="stat">
            <span>Wins</span>
            <strong class="wins-value">0</strong>
          </div>
          <div class="stat">
            <span>Losses</span>
            <strong class="losses-value">0</strong>
          </div>
          <div class="stat">
            <span>Win Rate</span>
            <strong class="win-rate-value">n/a</strong>
          </div>
          <div class="stat">
            <span>Total Trades</span>
            <strong class="trade-count-value">0</strong>
          </div>
        </div>

        <div class="traits-grid">
          <div class="trait">
            <span>Aggressive</span>
            <strong class="trait-aggressive">n/a</strong>
          </div>
          <div class="trait">
            <span>Chop Block</span>
            <strong class="trait-chop">n/a</strong>
          </div>
          <div class="trait">
            <span>Max Hold</span>
            <strong class="trait-hold">n/a</strong>
          </div>
          <div class="trait">
            <span>Rule / Net</span>
            <strong class="trait-weights">n/a</strong>
          </div>
        </div>

        <div class="message last-trade-box">No trades yet.</div>
        <div class="message diagnostics-box">Waiting for live diagnostics...</div>

        <div class="links-row">
          <a class="svg-link" target="_blank" rel="noreferrer">SVG</a>
          <a class="json-link" target="_blank" rel="noreferrer">Activations</a>
          <a class="report-link" target="_blank" rel="noreferrer">Report</a>
        </div>
      `;
      return card;
    }

    function positionLabel(position) {
      if (!position || !position.side || position.side === "flat") {
        return "flat";
      }
      return `${position.side} @ ${formatMoney(position.entry_price)}`;
    }

    function mergeStats(activations, report) {
      const reportStats = report
        ? {
            total_trades: report.total_trades,
            wins: report.wins,
            losses: report.losses,
            win_rate: report.win_rate,
          }
        : null;
      return reportStats || activations?.live_stats || {};
    }

    function renderLastTrade(lastTrade) {
      if (!lastTrade) {
        return "No trades yet.";
      }
      const result = String(lastTrade.result || "flat").toLowerCase();
      const pnl = lastTrade.pnl_fee_aware ?? lastTrade.pnl;
      return `
        <span class="result-pill ${escapeHtml(result)}">${escapeHtml(lastTrade.result || "FLAT")}</span>
        pnl ${escapeHtml(formatMoney(pnl))}
        <span class="muted">reason ${escapeHtml(lastTrade.reason || "n/a")}</span>
      `;
    }

    function renderDiagnostics(activations, report, activationError, reportError) {
      const signal = activations?.latest_signal;
      const chunks = [];
      if (signal) {
        chunks.push(
          `signal=${escapeHtml(signal.action_candidate || "hold")}`,
          `reason=${escapeHtml(signal.reason || "n/a")}`,
          `executed=${escapeHtml(String(Boolean(signal.executed)))}`,
        );
        if (signal.block_reason) {
          chunks.push(`block=${escapeHtml(signal.block_reason)}`);
        }
      }
      if (activationError) {
        chunks.push(`activations=${escapeHtml(activationError)}`);
      }
      if (reportError) {
        chunks.push(`report=${escapeHtml(reportError)}`);
      }
      if (report?.halt_reason) {
        chunks.push(`session_halt=${escapeHtml(report.halt_reason)}`);
      }
      return chunks.length ? chunks.join(" | ") : "Polling active. Waiting for first update...";
    }

    function setBar(card, selector, labelSelector, value) {
      const numeric = Number(value);
      const safeValue = Number.isFinite(numeric) ? Math.max(0, Math.min(1, numeric)) : 0;
      card.querySelector(selector).style.width = `${safeValue * 100}%`;
      card.querySelector(labelSelector).textContent = Number.isFinite(numeric) ? `${(numeric * 100).toFixed(1)}%` : "n/a";
    }

    function updateCard(instance, activationsResult, reportResult, svgResult) {
      const card = document.querySelector(`[data-instance-id="${instance.instance_id}"]`);
      const activations = activationsResult.ok ? activationsResult.data : null;
      const report = reportResult.ok ? reportResult.data : null;
      const svgText = svgResult.ok ? svgResult.data : null;
      const freshness = dataFreshnessLabel(activations?.updated_at);
      const generationMismatch =
        (activations && Number(activations.generation) !== Number(DASHBOARD_GENERATION)) ||
        (report && Number(report.generation) !== Number(DASHBOARD_GENERATION));

      card.querySelector(".profile-label").textContent = activations?.profile_name || report?.profile_name || "waiting for first update...";
      const freshnessPill = card.querySelector(".freshness-pill");
      freshnessPill.textContent = generationMismatch ? "gen mismatch" : freshness.text;
      freshnessPill.className = `status-pill freshness-pill ${generationMismatch ? "halted" : freshness.className}`;

      card.querySelector(".market-pill").textContent = `state: ${activations?.current_market_state || "UNKNOWN"}`;
      card.querySelector(".position-pill").textContent = `position: ${positionLabel(activations?.current_position)}`;
      card.querySelector(".halt-pill").textContent = `halted: ${activations?.halt_reason ? "yes" : "no"}`;
      card.querySelector(".generation-pill").textContent = generationMismatch
        ? `gen: ${activations?.generation || report?.generation || "?"}`
        : `gen: ${DASHBOARD_GENERATION_LABEL}`;

      card.querySelector(".equity-value").textContent = formatMoney(activations?.current_equity);
      card.querySelector(".price-value").textContent = formatMoney(activations?.current_price);
      card.querySelector(".signal-value").textContent = activations?.latest_signal
        ? `${activations.latest_signal.action_candidate || "hold"} / ${activations.latest_signal.reason || "n/a"}`
        : "waiting";
      card.querySelector(".halt-reason-value").textContent = activations?.halt_reason || report?.halt_reason || "none";

      setBar(card, ".prob-long-bar", ".prob-long-label", activations?.network_scores?.prob_win_long);
      setBar(card, ".prob-short-bar", ".prob-short-label", activations?.network_scores?.prob_win_short);

      const stats = mergeStats(activations, report);
      card.querySelector(".wins-value").textContent = stats.wins ?? 0;
      card.querySelector(".losses-value").textContent = stats.losses ?? 0;
      card.querySelector(".win-rate-value").textContent = formatPercent(stats.win_rate);
      card.querySelector(".trade-count-value").textContent = stats.total_trades ?? 0;

      const traits = activations?.profile_traits || {};
      card.querySelector(".trait-aggressive").textContent =
        traits.aggressive_entries === undefined ? "n/a" : String(traits.aggressive_entries);
      card.querySelector(".trait-chop").textContent =
        traits.block_entries_in_chop === undefined ? "n/a" : String(traits.block_entries_in_chop);
      card.querySelector(".trait-hold").textContent =
        traits.max_hold_seconds === undefined ? "n/a" : `${traits.max_hold_seconds}s`;
      card.querySelector(".trait-weights").textContent =
        traits.rule_weight === undefined || traits.weight_network === undefined
          ? "n/a"
          : `${Number(traits.rule_weight).toFixed(2)} / ${Number(traits.weight_network).toFixed(2)}`;

      card.querySelector(".last-trade-box").innerHTML = renderLastTrade(activations?.last_trade);
      card.querySelector(".diagnostics-box").innerHTML = renderDiagnostics(
        activations,
        report,
        activationsResult.ok ? "" : activationsResult.error,
        reportResult.ok ? "" : reportResult.error,
      );

      const svgHost = card.querySelector(".svg-host");
      const placeholder = card.querySelector(".svg-placeholder");
      if (svgText) {
        svgHost.innerHTML = svgText;
        placeholder.style.display = "none";
      } else {
        svgHost.innerHTML = "";
        placeholder.style.display = "flex";
        placeholder.textContent = activationsResult.ok
          ? "network.svg unavailable. Waiting for visualization..."
          : "waiting for first update...";
      }

      card.querySelector(".svg-link").href = instance.paths.network_svg;
      card.querySelector(".json-link").href = instance.paths.activations_latest;
      card.querySelector(".report-link").href = instance.paths.session_report;
    }

    function updateSummary(instances, results) {
      let liveCount = 0;
      let totalEquity = 0;
      let openPositions = 0;
      let halted = 0;
      let reportsReady = 0;

      for (const instance of instances) {
        const result = results[instance.instance_id];
        const activations = result?.activations?.ok ? result.activations.data : null;
        const report = result?.report?.ok ? result.report.data : null;
        if (activations) {
          liveCount += 1;
          totalEquity += Number(activations.current_equity || 0);
          if (activations.current_position?.side && activations.current_position.side !== "flat") {
            openPositions += 1;
          }
          if (activations.halt_reason) {
            halted += 1;
          }
        }
        if (report) {
          reportsReady += 1;
        }
      }

      document.getElementById("summary-live").textContent = `${liveCount} / ${instances.length}`;
      document.getElementById("summary-equity").textContent = formatMoney(totalEquity);
      document.getElementById("summary-open").textContent = String(openPositions);
      document.getElementById("summary-halted").textContent = String(halted);
      document.getElementById("summary-reports").textContent = String(reportsReady);
    }

    async function pollOnce() {
      if (!manifest) {
        return;
      }
      const results = {};
      const fetches = manifest.instances.map(async (instance) => {
        const [activations, report, svg] = await Promise.all([
          safeFetchJson(instance.paths.activations_latest),
          safeFetchJson(instance.paths.session_report),
          safeFetchText(instance.paths.network_svg),
        ]);
        results[instance.instance_id] = { activations, report, svg };
        updateCard(instance, activations, report, svg);
      });

      await Promise.all(fetches);
      updateSummary(manifest.instances, results);
      document.getElementById("last-refresh").textContent = new Date().toLocaleString();
    }

    async function bootstrap() {
      const manifestResult = await safeFetchJson(manifestUrl);
      if (!manifestResult.ok) {
        document.getElementById("swarm-grid").innerHTML =
          `<div class="message">Failed to load dashboard manifest: ${escapeHtml(manifestResult.error)}</div>`;
        return;
      }

      manifest = manifestResult.data;
      const swarmGrid = document.getElementById("swarm-grid");
      swarmGrid.innerHTML = "";
      for (const instance of manifest.instances) {
        swarmGrid.appendChild(buildCard(instance));
      }

      await pollOnce();
      window.setInterval(pollOnce, POLL_INTERVAL_MS);
    }

    bootstrap();
  </script>
</body>
</html>
"""
    return (
        template.replace("__GENERATION_DISPLAY__", f"{generation:03d}")
        .replace("__GENERATION_VALUE__", str(generation))
        .replace("__POLL_MS__", str(poll_interval_ms))
    )


def write_swarm_dashboard(root_dir: Path, generation: int, instance_ids: list[str]) -> Path:
    dashboard_dir = _dashboard_dir(root_dir, generation)
    dashboard_dir.mkdir(parents=True, exist_ok=True)

    manifest = _build_manifest(root_dir, generation, instance_ids)
    manifest_path = dashboard_dir / "dashboard_manifest.json"
    save_json(manifest_path, manifest)

    html_path = dashboard_dir / "dashboard.html"
    html_path.write_text(_render_dashboard_html(generation), encoding="utf-8")
    return html_path


def _discover_repo_root(path: Path) -> Path:
    resolved = path.resolve()
    candidates = [resolved.parent, *resolved.parents]
    for candidate in candidates:
        if (candidate / "main.py").exists() and (candidate / "src" / "eth_bot").exists():
            return candidate
    return resolved.parent


def serve_dashboard(path: Path, port: int = 8000) -> None:
    dashboard_path = path.resolve()
    repo_root = _discover_repo_root(dashboard_path)
    relative_dashboard = dashboard_path.relative_to(repo_root).as_posix()
    handler = partial(SimpleHTTPRequestHandler, directory=str(repo_root))

    with ThreadingHTTPServer(("127.0.0.1", port), handler) as server:
        print(f"Serving dashboard root: {repo_root}")
        print(f"Dashboard URL: http://127.0.0.1:{port}/{relative_dashboard}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("Dashboard server stopped.")
