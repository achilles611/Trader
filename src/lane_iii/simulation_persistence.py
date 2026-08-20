"""Durable, hash-checked persistence for Lane III Phase E simulations only.

The store deliberately persists immutable simulation snapshots rather than any
broker state.  A hash mismatch is an integrity error and recovery fails closed;
it never returns a synthetic FLAT state.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .simulated_execution import (
    DeterministicMNQSimulator,
    SimulationConfig,
    SimulationRecoveryRefused,
)


class SimulationPersistenceConflict(RuntimeError):
    """A run identity is being reused for different immutable simulation truth."""


class _ClosingConnection(sqlite3.Connection):
    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> bool | None:
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


class SimulationStateStore:
    """One bounded latest-snapshot record per deterministic simulation run.

    The snapshot includes the append-only ledger, orders, fills, position,
    risk/operator state, admissions, and configuration identity.  Callers may
    checkpoint after any replay event.  No credentials or external account
    attributes can be stored through this surface.
    """

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS l3e_simulation_snapshots (
                    run_id TEXT PRIMARY KEY,
                    configuration_hash TEXT NOT NULL,
                    state_json TEXT NOT NULL,
                    state_hash TEXT NOT NULL,
                    ledger_hash TEXT NOT NULL
                )"""
            )

    def checkpoint(self, simulator: DeterministicMNQSimulator) -> str:
        if type(simulator) is not DeterministicMNQSimulator:
            raise ValueError("Checkpoint requires an exact L3-E simulator.")
        snapshot = simulator.snapshot()
        state = snapshot["state"]
        state_hash = snapshot["state_hash"]
        if not isinstance(state, dict) or not isinstance(state_hash, str):  # defensive, impossible with exact simulator
            raise SimulationPersistenceConflict("Simulator produced an invalid persistence snapshot.")
        rendered = json.dumps(snapshot, sort_keys=True, separators=(",", ":"))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            prior = connection.execute(
                "SELECT configuration_hash, state_hash FROM l3e_simulation_snapshots WHERE run_id=?", (simulator.run_id,)
            ).fetchone()
            if prior is not None and prior["configuration_hash"] != simulator.config.configuration_hash:
                connection.rollback()
                raise SimulationPersistenceConflict("A simulation run cannot change configuration identity.")
            connection.execute(
                """INSERT INTO l3e_simulation_snapshots(run_id, configuration_hash, state_json, state_hash, ledger_hash)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(run_id) DO UPDATE SET
                    state_json=excluded.state_json, state_hash=excluded.state_hash, ledger_hash=excluded.ledger_hash""",
                (simulator.run_id, simulator.config.configuration_hash, rendered, state_hash, simulator.ledger_hash),
            )
            connection.commit()
        return state_hash

    def recover(self, config: SimulationConfig, run_id: str) -> DeterministicMNQSimulator:
        if type(config) is not SimulationConfig or not isinstance(run_id, str) or not run_id:
            raise ValueError("Recovery requires exact configuration and simulation run identity.")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT configuration_hash, state_json, state_hash, ledger_hash FROM l3e_simulation_snapshots WHERE run_id=?", (run_id,)
            ).fetchone()
        if row is None:
            raise SimulationRecoveryRefused("No persisted simulation state exists; UNKNOWN is not FLAT.")
        if row["configuration_hash"] != config.configuration_hash:
            raise SimulationRecoveryRefused("Persisted simulation uses a different configuration identity.")
        try:
            snapshot = json.loads(row["state_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise SimulationRecoveryRefused("Persisted simulation JSON is corrupt; recovery fails closed.") from exc
        if not isinstance(snapshot, dict) or snapshot.get("state_hash") != row["state_hash"]:
            raise SimulationRecoveryRefused("Persisted simulation state hash is inconsistent.")
        simulator = DeterministicMNQSimulator.from_snapshot(config, snapshot)
        if simulator.ledger_hash != row["ledger_hash"]:
            raise SimulationRecoveryRefused("Persisted simulation ledger hash is inconsistent.")
        return simulator

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, factory=_ClosingConnection)
        connection.row_factory = sqlite3.Row
        return connection
