"""Provision a short-lived, disarmed Sim101 mechanical-test capability.

This is deliberately not a live-capital provisioner.  It records the observed
simulation identity, creates only ``live_capital=False`` capability material,
and writes the native binding needed for the installed AddOn to authenticate.
It never opens a listener, starts NinjaTrader, arms an AddOn, or sends an
order.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import timedelta
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from uuid import uuid4


REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from src.l3h_live.bootstrap import AccountEvidence, NativeCapabilityBinding, classify_account, create_attestation
from src.l3h_live.contracts import AccountClass, LiveCapability, canonical_hash, canonical_json, parse_utc, utc_now
from src.l3h_live.risk import LiveCanaryRiskProfile


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _addon_fingerprint(path: Path) -> str:
    source = path.read_text(encoding="utf-8")
    normalized = re.sub(
        r'private const string SourceFingerprint = "[^"]+";',
        'private const string SourceFingerprint = "SOURCE_FINGERPRINT_PLACEHOLDER";',
        source,
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _atomic_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(canonical_json(value))
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create only a disarmed Sim101 mechanical-test capability.")
    parser.add_argument("--authority-root", type=Path, default=Path(os.environ["LOCALAPPDATA"]) / "Beelzebub" / "authority" / "l3h")
    parser.add_argument("--account-name", default="Sim101")
    parser.add_argument("--connection-name", default="LucidFlex25k")
    parser.add_argument("--provider-name", default="Tradovate")
    parser.add_argument("--provider-program", default="Simulation")
    parser.add_argument("--addon-source", type=Path, default=Path.home() / "Documents" / "NinjaTrader 8" / "bin" / "Custom" / "AddOns" / "BeelzebubLiveExecutionAddOn.cs")
    parser.add_argument("--ninjatrader-executable", type=Path, default=Path(r"C:\Program Files\NinjaTrader 8\bin\NinjaTrader.exe"))
    parser.add_argument("--valid-hours", type=int, default=8)
    args = parser.parse_args()

    if args.valid_hours < 1 or args.valid_hours > 24:
        raise ValueError("Mechanical capability validity must be between 1 and 24 hours.")
    root = args.authority_root.expanduser().resolve()
    capability_root = root / "capabilities"
    capability_key_path = root / "keys" / "l3h.capability.hmac.key"
    gateway_key_path = root / "keys" / "l3h.execution.local.key"
    if not capability_root.is_dir() or not capability_key_path.is_file() or not gateway_key_path.is_file():
        raise ValueError("L3H bootstrap material is incomplete; run l3h_bootstrap.ps1 first.")
    if not args.addon_source.is_file() or not args.ninjatrader_executable.is_file():
        raise ValueError("Installed AddOn source or NinjaTrader executable is unavailable.")

    observed_at = utc_now()
    evidence = AccountEvidence(
        account_identifier=args.account_name,
        display_name=args.account_name,
        connection_name=args.connection_name,
        provider_name=args.provider_name,
        provider_program=args.provider_program,
        metadata_class=AccountClass.LOCAL_SIMULATION,
        observed_at=observed_at,
    )
    classification, reasons = classify_account(evidence)
    if classification is not AccountClass.LOCAL_SIMULATION:
        raise ValueError("Only a concordantly classified LOCAL_SIMULATION account can receive this capability.")

    capability_key = capability_key_path.read_bytes()
    gateway_key = gateway_key_path.read_bytes()
    if len(capability_key) < 32 or len(gateway_key) < 32:
        raise ValueError("L3H local key material is too short.")
    token = uuid4().hex
    created_at = observed_at
    expires_at = (parse_utc(created_at, "created") + timedelta(hours=args.valid_hours)).isoformat().replace("+00:00", "Z")
    source_fingerprint = _addon_fingerprint(args.addon_source)
    capability = LiveCapability(
        schema="lane-iii-phase-h-live-capability-v1",
        capability_id="l3h-cap-sim101-" + token,
        created_at=created_at,
        expires_at=expires_at,
        account_alias="Sim101",
        account_binding_hash=evidence.binding_hash,
        account_class=AccountClass.LOCAL_SIMULATION,
        connection_identity_hash=canonical_hash({"connection_name": args.connection_name, "provider_name": args.provider_name, "provider_program": args.provider_program}),
        native_instrument="MNQ SEP26",
        canonical_contract="MNQU6",
        exchange="CME",
        tick_size=Decimal("0.25"),
        tick_value_dollars=Decimal("0.50"),
        point_value_dollars=Decimal("2.00"),
        maximum_quantity=1,
        live_capital=False,
        policy_hash=canonical_hash({"policy": "l3h-sim101-mechanical-commissioning-v1", "live_capital": False}),
        risk_hash=LiveCanaryRiskProfile().configuration_hash,
        prop_rule_hash=canonical_hash({"provider_program": args.provider_program, "account_class": AccountClass.LOCAL_SIMULATION.value}),
        strategy_artifact_hash=canonical_hash({"strategy": "MECHANICAL_SIM101_NO_LIVE_STRATEGY"}),
        source_fingerprint=source_fingerprint,
        ninjatrader_build_fingerprint=_sha256_file(args.ninjatrader_executable),
        allowed_session_profiles=("SIM101_MECHANICAL_ONLY",),
        commissioning_epoch="l3h-sim101-mechanical-" + token,
        activation_nonce_family="l3h-activation-sim101-" + token,
    ).signed(capability_key)
    attestation = create_attestation(
        evidence, AccountClass.LOCAL_SIMULATION, operator_confirmation=True,
        attestation_id="l3h-attestation-sim101-" + token,
    ).signed(capability_key)
    binding = NativeCapabilityBinding(
        "lane-iii-phase-h-native-binding-v1", args.account_name, evidence.binding_hash,
        capability.capability_hash, capability.capability_id, capability.commissioning_epoch,
    ).signed(gateway_key)
    _atomic_json(capability_root / (capability.capability_id + ".json"), asdict(capability))
    _atomic_json(capability_root / (capability.capability_id + ".attestation.json"), asdict(attestation))
    _atomic_json(root / "l3h.live.binding.json", asdict(binding))
    print(json.dumps({
        "account_class": classification.value, "classification_evidence": list(reasons),
        "account_binding_hash": evidence.binding_hash, "capability_id": capability.capability_id,
        "capability_hash": capability.capability_hash, "commissioning_epoch": capability.commissioning_epoch,
        "source_fingerprint": source_fingerprint, "live_capital": False, "live_armed": False,
        "expires_at": capability.expires_at,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
