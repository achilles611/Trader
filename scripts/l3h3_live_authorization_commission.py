"""Commission the L3H.3 permission boundary without sending a live order.

The successful local exercise uses deterministic synthetic live-account facts
only to test the permission architecture.  It never loads brokerage
credentials, starts the live gateway, dispatches a command, or calls the
native order seam.  Real account identity therefore remains UNVERIFIED until
a later operator-supervised identity ceremony is performed.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory


REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from src.l3h_live.contracts import canonical_hash, canonical_json, utc_now
from src.l3h_live.event_store import LiveEventStore
from src.l3h_live.live_authorization import (
    AuthorizationAccountClass, AuthorizationFacts, HumanAuthorization,
    LiveAuthorizationBoundary, LiveAuthorizationState, identity_from_native_metadata,
)
from src.l3h_live.status import fail_closed_status


def _git_head() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPOSITORY, check=True,
        capture_output=True, text=True, encoding="utf-8",
    )
    return result.stdout.strip()


def _default_mechanical_status() -> Path:
    local = os.environ.get("LOCALAPPDATA")
    if not local:
        return Path("MISSING_LOCALAPPDATA")
    return Path(local) / "Beelzebub" / "authority" / "l3h" / "events" / "l3h-gateway-status.json"


def _synthetic_facts(build_identity: str, addon_provenance: str, observed_at: str) -> AuthorizationFacts:
    metadata = {
        "account_id": "SYNTHETIC-NOT-A-BROKER-ID", "account_name": "SyntheticLiveBoundary",
        "display_name": "Synthetic Live Boundary", "fcm": "SYNTHETIC",
        "account_provider": "ProviderSynthetic", "account_status": "Enabled",
        "connection_name": "SyntheticConnection", "connection_provider": "ProviderSynthetic",
        "connection_brand": "Synthetic", "connection_type": "DeterministicStub",
        "connection_mode": "Live", "connection_is_demo": False,
        "connection_can_manage_orders": True, "connection_status": "Connected",
    }
    identity = identity_from_native_metadata(
        metadata, safe_account_id="SYNTHETIC-LIVE-STUB",
        account_class=AuthorizationAccountClass.LIVE_CAPITAL, observed_at=observed_at,
    )
    return AuthorizationFacts(
        observed_at=observed_at, account=identity, native_instrument="MNQ SEP26", canonical_contract="MNQU6",
        maximum_quantity=1, position="FLAT", quantity=0, owned_working_entry_orders=0,
        unresolved_owned_protective_orders=0, foreign_or_unknown_activity=0,
        gateway_authenticated=True, gateway_session_id="l3h3-gateway-session-synthetic",
        addon_session_id="l3h3-addon-session-synthetic", addon_provenance=addon_provenance,
        addon_build_identity=addon_provenance, provider_connected=True, connection_fresh=True,
        reconciliation_status="PASS", reconciliation_observed_at=observed_at, protection_status="PASS",
        command_kill_ready=True, native_menu_kill_ready=True, out_of_band_kill_ready=True,
        beelzebub_build_identity=build_identity, strategy_runtime_identity="SYNTHETIC_NO_STRATEGY_AUTHORITY",
        runtime_session_id="l3h3-runtime-session-synthetic",
    )


def _exercise_boundary() -> dict[str, object]:
    now = utc_now()
    head = _git_head()
    build_identity = canonical_hash({"git_head": head, "boundary": "L3H3_ONE_SHOT_V1"})
    addon_source = (REPOSITORY / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubLiveExecutionAddOn.cs").read_bytes()
    addon_provenance = canonical_hash({"source_sha256": __import__("hashlib").sha256(addon_source).hexdigest()})
    facts = _synthetic_facts(build_identity, addon_provenance, now)
    with TemporaryDirectory(prefix="l3h3-boundary-") as directory:
        store = LiveEventStore(Path(directory) / "l3h3.sqlite3")
        boundary = LiveAuthorizationBoundary(
            store, native_hmac_key=b"l3h3-synthetic-native-key-32bytes!",
            beelzebub_build_identity=build_identity, expected_addon_provenance=addon_provenance,
        )
        preflight = boundary.start_preflight(facts, now=now)
        if preflight.blockers or boundary.state is not LiveAuthorizationState.PREFLIGHT_READY:
            raise RuntimeError("Synthetic L3H.3 preflight failed: " + ",".join(preflight.blockers))
        challenge = boundary.begin_authorization(
            preflight_id=preflight.preflight_id, preflight_digest=preflight.digest, now=now,
        )
        authorization = HumanAuthorization(
            preflight_id=challenge.preflight_id, preflight_digest=challenge.preflight_digest,
            challenge=challenge.challenge, safe_account_id=challenge.safe_account_id,
            account_class=challenge.account_class, native_instrument=challenge.native_instrument,
            quantity=challenge.quantity, authority_type=challenge.authority_type,
            acknowledgement=challenge.acknowledgement, actor_type="LOCAL_HUMAN", local_transport=True,
        )
        capability_id = boundary.authorize(authorization, now=now)
        if boundary.state is not LiveAuthorizationState.CANARY_AUTHORIZED:
            raise RuntimeError("Synthetic L3H.3 authorization did not reach one-shot authority.")
        # This commissioning pass deliberately stops before atomic admission,
        # command sealing, gateway dispatch, or native submission.
        boundary.disarm("L3H3_COMMISSIONING_STOP_BEFORE_LIVE_ADMISSION")
        chain_valid, chain_reason = store.verify()
        if not chain_valid:
            raise RuntimeError("L3H.3 transition evidence failed verification: " + chain_reason)
        return {
            "synthetic_preflight_digest": preflight.digest,
            "synthetic_capability_id_hash": canonical_hash(capability_id),
            "transition_chain": chain_reason,
            "final_state": boundary.state.value,
            "build_identity": build_identity,
            "addon_source_identity": addon_provenance,
        }


def _bypass_audit() -> dict[str, object]:
    live_addon = (REPOSITORY / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubLiveExecutionAddOn.cs").read_text(encoding="utf-8")
    paper_addon = (REPOSITORY / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubPaperExecutionAddOn.cs").read_text(encoding="utf-8")
    gateway = (REPOSITORY / "src" / "l3h_live" / "gateway.py").read_text(encoding="utf-8")
    runtime = (REPOSITORY / "src" / "l3h_live" / "runtime.py").read_text(encoding="utf-8")
    control = (REPOSITORY / "src" / "copytrade" / "control_center.py").read_text(encoding="utf-8")
    native_addons = list((REPOSITORY / "ninjatrader" / "NinjaScript" / "AddOns").glob("*.cs"))
    submitters = sorted(path.name for path in native_addons if ".Submit(" in path.read_text(encoding="utf-8"))
    checks = {
        "native_submitters_are_known": submitters == ["BeelzebubLiveExecutionAddOn.cs", "BeelzebubPaperExecutionAddOn.cs"],
        "live_entry_has_one_shot_gate": "ValidateAndConsumeLiveAuthorization(command, out reason)" in live_addon,
        "live_native_hmac_envelope": "lane-iii-phase-h-live-admission-v1" in live_addon,
        "live_native_identity_rechecked": "NativeAccountIdentityReady()" in live_addon,
        "generic_arm_not_live_authority": "DENY_LIVE_REQUIRES_ONE_SHOT_AUTHORIZATION" in live_addon and "private bool armed" not in live_addon,
        "native_live_send_sentinel": "liveSendCount" in live_addon,
        "paper_submitter_is_sim101_only": "Sim101" in paper_addon and "LOCAL_SIMULATION" in paper_addon,
        "gateway_requires_envelope": "_validate_entry_authority" in gateway and "verify_native_admission_envelope" in gateway,
        "runtime_carries_native_envelope": '"live_authorization": envelope.as_mapping()' in runtime,
        "no_browser_activation_route": '/api/lane-iii/live/activate' not in control,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {"status": "PASS" if not failed else "BLOCKED_AUTHORIZATION_BYPASS", "checks": checks, "failed": failed, "native_submitters": submitters}


def _atomic_write(path: Path, value: dict[str, object]) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_bytes(canonical_json(value))
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mechanical-status", type=Path, default=_default_mechanical_status())
    parser.add_argument("--output", type=Path)
    parser.add_argument("--status-output", type=Path)
    parser.add_argument("--operator", action="store_true", help="Also print the compact operator field list to stderr.")
    args = parser.parse_args()

    exercise = _exercise_boundary()
    bypass_audit = _bypass_audit()
    mechanical = fail_closed_status(mechanical_status_path=args.mechanical_status)
    mechanical_pass = mechanical.get("terminal_status") == "L3H_MECHANICALLY_COMMISSIONED"
    result: dict[str, object] = {
        "schema": "lane-iii-phase-h3-commissioning-result-v1",
        "terminal_status": "BLOCKED_LIVE_ACCOUNT_IDENTITY",
        "mechanical_commissioning": "PASS" if mechanical_pass else "BLOCKED",
        "live_account_identity": "UNVERIFIED",
        "account_class": "UNKNOWN",
        "authorized_account": None,
        "authorization_boundary": "IMPLEMENTED",
        "live_authorization_boundary": "IMPLEMENTED",
        "live_authority": "DISARMED",
        "live_canary": "NOT_RUN",
        "contract": "MNQ SEP26",
        "maximum_quantity": 1,
        "position": "UNVERIFIED_LIVE_ACCOUNT",
        "working_orders": "UNVERIFIED_LIVE_ACCOUNT",
        "gateway": "AUTHENTICATED_LOOPBACK_L3H2" if mechanical_pass else "UNVERIFIED",
        "addon_provenance": "L3H3_SOURCE_IMPLEMENTED_RUNTIME_UNVERIFIED",
        "reconciliation": "PASS_SIM101_ONLY" if mechanical_pass else "UNVERIFIED",
        "protection": "PASS_SIM101_ONLY" if mechanical_pass else "UNVERIFIED",
        "kill_paths": "PASS_SIM101_ONLY" if mechanical_pass else "UNVERIFIED",
        "quarantine": False,
        "locked": False,
        "preflight_age_seconds": None,
        "authorization_expires_at": None,
        "live_send_count": 0,
        "bypass_audit": bypass_audit,
        "synthetic_boundary_exercise": exercise,
        "real_provider_interaction": "NOT_PERFORMED",
        "generated_at": utc_now(),
        "implementation_head": _git_head(),
    }
    if args.output:
        _atomic_write(args.output, result)
    if args.status_output:
        _atomic_write(args.status_output, result)
    print(json.dumps(result, sort_keys=True))
    if args.operator:
        for key in (
            "mechanical_commissioning", "live_account_identity", "live_authorization_boundary", "live_authority",
            "live_canary", "contract", "maximum_quantity", "gateway", "addon_provenance", "reconciliation",
            "protection", "kill_paths", "live_send_count", "terminal_status",
        ):
            print(f"{key.upper()}: {result[key]}", file=sys.stderr)
    return 2 if not mechanical_pass or bypass_audit["status"] != "PASS" else 0


if __name__ == "__main__":
    raise SystemExit(main())
