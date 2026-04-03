from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import InstancePaths, StrategyProfile
from .models import NetworkScores
from .network import NeuralNetwork


LAYER_LABELS = ("Input", "Hidden 1", "Hidden 2", "Hidden 3", "Hidden 4", "Output")


def _line_color(weight: float) -> str:
    return "#157f1f" if weight >= 0 else "#b22222"


def _line_width(weight: float) -> float:
    return max(0.5, min(4.5, abs(weight) * 8))


def _profile_traits_payload(profile: StrategyProfile) -> dict[str, Any]:
    return {
        "aggressive_entries": profile.aggressive_entries,
        "block_entries_in_chop": profile.block_entries_in_chop,
        "max_hold_seconds": profile.max_hold_seconds,
        "rule_weight": profile.rule_weight,
        "weight_network": profile.weight_network,
        "entry_threshold_long": profile.entry_threshold_long,
        "entry_threshold_short": profile.entry_threshold_short,
        "min_confirmation_signals": profile.min_confirmation_signals,
    }


def save_network_bundle(
    network: NeuralNetwork,
    paths: InstancePaths,
    *,
    instance_id: str,
    family: str,
    generation: int,
    profile_name: str,
    profile: StrategyProfile,
    network_scores: NetworkScores | None = None,
    current_market_state: str | None = None,
    current_equity: float | None = None,
    current_price: float | None = None,
    halt_reason: str | None = None,
    current_position: dict[str, Any] | None = None,
    live_stats: dict[str, Any] | None = None,
    latest_signal: dict[str, Any] | None = None,
    updated_at: str | None = None,
    last_trade: dict[str, Any] | None = None,
) -> None:
    payload = network.to_json()
    paths.network_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    paths.network_json_path.parent.mkdir(parents=True, exist_ok=True)
    paths.activations_path.parent.mkdir(parents=True, exist_ok=True)
    paths.network_viz_path.parent.mkdir(parents=True, exist_ok=True)

    paths.network_snapshot_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    paths.network_json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    activations_payload = {
        "instance_id": instance_id,
        "family": family,
        "generation": generation,
        "profile_name": profile_name,
        "updated_at": updated_at,
        "current_market_state": current_market_state,
        "current_equity": current_equity,
        "current_price": current_price,
        "halt_reason": halt_reason,
        "current_position": current_position,
        "network_scores": network_scores.to_json() if network_scores else None,
        "profile_traits": _profile_traits_payload(profile),
        "live_stats": live_stats,
        "latest_signal": latest_signal,
        "last_trade": last_trade,
    }
    paths.activations_path.write_text(json.dumps(activations_payload, indent=2), encoding="utf-8")
    paths.network_viz_path.write_text(
        render_network_svg(
            network,
            instance_id=instance_id,
            family=family,
            generation=generation,
            profile_name=profile_name,
            profile=profile,
            network_scores=network_scores,
            current_market_state=current_market_state,
            current_equity=current_equity,
            last_trade=last_trade,
        ),
        encoding="utf-8",
    )


def render_network_svg(
    network: NeuralNetwork,
    *,
    instance_id: str,
    family: str,
    generation: int,
    profile_name: str,
    profile: StrategyProfile,
    network_scores: NetworkScores | None = None,
    current_market_state: str | None = None,
    current_equity: float | None = None,
    last_trade: dict[str, Any] | None = None,
) -> str:
    layer_sizes = list(network.config.layer_sizes)
    width = 1200
    height = 720
    padding_x = 90
    padding_y = 80
    drawable_width = width - (padding_x * 2)
    drawable_height = 420
    layer_spacing = drawable_width / max(1, len(layer_sizes) - 1)

    node_positions: list[list[tuple[float, float]]] = []
    for layer_index, layer_size in enumerate(layer_sizes):
        x = padding_x + (layer_index * layer_spacing)
        if layer_size == 1:
            y_positions = [padding_y + (drawable_height / 2)]
        else:
            step = drawable_height / max(1, layer_size - 1)
            y_positions = [padding_y + (node_index * step) for node_index in range(layer_size)]
        node_positions.append([(x, y) for y in y_positions])

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#fbfaf7" />',
        '<text x="40" y="36" font-size="24" font-family="Consolas, monospace" fill="#1f2933">'
        f"{instance_id} ({family}) g{generation} - {profile_name}</text>",
        '<text x="40" y="64" font-size="14" font-family="Consolas, monospace" fill="#52606d">'
        f"market_state={current_market_state or 'UNKNOWN'} equity={current_equity if current_equity is not None else 'n/a'}</text>",
    ]

    if network_scores is not None:
        parts.append(
            '<text x="40" y="92" font-size="14" font-family="Consolas, monospace" fill="#102a43">'
            f"prob_win_long={network_scores.prob_win_long:.3f} prob_win_short={network_scores.prob_win_short:.3f}</text>"
        )

    if last_trade:
        parts.append(
            '<text x="40" y="120" font-size="14" font-family="Consolas, monospace" fill="#7b341e">'
            f"last_trade={last_trade.get('result', 'n/a')} pnl={last_trade.get('pnl_fee_aware', 0):.2f}</text>"
        )

    for layer_index, layer_weights in enumerate(network.weights):
        for output_index, neuron_weights in enumerate(layer_weights):
            x2, y2 = node_positions[layer_index + 1][output_index]
            for input_index, weight in enumerate(neuron_weights):
                x1, y1 = node_positions[layer_index][input_index]
                parts.append(
                    f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
                    f'stroke="{_line_color(weight)}" stroke-width="{_line_width(weight):.2f}" '
                    'stroke-opacity="0.45" />'
                )

    active_layers = network_scores.hidden_activations if network_scores else []
    for layer_index, positions in enumerate(node_positions):
        label = LAYER_LABELS[min(layer_index, len(LAYER_LABELS) - 1)]
        x_label = positions[0][0] - 24
        parts.append(
            f'<text x="{x_label:.1f}" y="{padding_y - 18:.1f}" font-size="13" '
            f'font-family="Consolas, monospace" fill="#334e68">{label} ({len(positions)})</text>'
        )
        for node_index, (x, y) in enumerate(positions):
            activation = None
            if 0 < layer_index < len(node_positions) - 1 and layer_index - 1 < len(active_layers):
                hidden = active_layers[layer_index - 1]
                if node_index < len(hidden):
                    activation = hidden[node_index]
            fill = "#d9e2ec"
            if activation is not None:
                intensity = max(0.0, min(1.0, activation))
                fill = f"rgb({int(217 - 80 * intensity)}, {int(226 - 40 * intensity)}, {int(236 - 120 * intensity)})"
            if layer_index == len(node_positions) - 1 and network_scores is not None:
                activation = network_scores.prob_win_long if node_index == 0 else network_scores.prob_win_short
                intensity = max(0.0, min(1.0, activation))
                fill = f"rgb({int(220 - 90 * intensity)}, {int(240 - 20 * intensity)}, {int(220 - 150 * intensity)})"
            parts.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="10" fill="{fill}" stroke="#486581" stroke-width="1.3" />'
            )

    traits_y = 560
    traits = [
        f"aggressive_entries={profile.aggressive_entries}",
        f"block_entries_in_chop={profile.block_entries_in_chop}",
        f"max_hold_seconds={profile.max_hold_seconds}",
        f"rule_weight={profile.rule_weight:.2f}",
        f"weight_network={profile.weight_network:.2f}",
        f"threshold_long={profile.entry_threshold_long:.2f}",
        f"threshold_short={profile.entry_threshold_short:.2f}",
    ]
    for index, trait in enumerate(traits):
        parts.append(
            '<text x="40" y="{}" font-size="14" font-family="Consolas, monospace" fill="#243b53">{}</text>'.format(
                traits_y + (index * 22),
                trait,
            )
        )

    parts.append("</svg>")
    return "\n".join(parts)
