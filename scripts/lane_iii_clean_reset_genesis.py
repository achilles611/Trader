"""Create the one authorized Lane III clean-reset genesis record."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from src.l3g_paper.reset import create_clean_reset_genesis


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--epoch", required=True)
    parser.add_argument("--reset-receipt", required=True)
    parser.add_argument("--reset-timestamp", required=True)
    parser.add_argument("--checkout-sha", required=True)
    parser.add_argument("--build-sha", required=True)
    parser.add_argument("--runtime-sha", required=True)
    parser.add_argument("--addon-source-fingerprint", required=True)
    parser.add_argument("--addon-build-fingerprint", required=True)
    options = parser.parse_args()
    result = create_clean_reset_genesis(
        options.ledger,
        epoch_id=options.epoch,
        reset_receipt_path=options.reset_receipt,
        reset_timestamp=options.reset_timestamp,
        checkout_sha=options.checkout_sha,
        build_sha=options.build_sha,
        runtime_sha=options.runtime_sha,
        addon_source_fingerprint=options.addon_source_fingerprint,
        addon_build_fingerprint=options.addon_build_fingerprint,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
