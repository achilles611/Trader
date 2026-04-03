from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import BotState, ClosedTrade, utc_now


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def create_initial_state(starting_cash: float) -> BotState:
    today = utc_now().date().isoformat()
    return BotState(
        cash=starting_cash,
        peak_equity=starting_cash,
        day_start_equity=starting_cash,
        day_marker=today,
    )


def load_state(path: Path, starting_cash: float) -> BotState:
    if not path.exists():
        return create_initial_state(starting_cash)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return BotState.from_json(payload)


def save_state(path: Path, state: BotState) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(state.to_json(), indent=2), encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload))
        handle.write("\n")


def append_trade(path: Path, trade: ClosedTrade) -> None:
    append_jsonl(path, trade.to_json())


def append_training_sample(path: Path, payload: dict[str, Any]) -> None:
    append_jsonl(path, payload)


def save_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
