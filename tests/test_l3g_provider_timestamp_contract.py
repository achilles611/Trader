from __future__ import annotations

from dataclasses import replace
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
import unittest

from src.l3f_provider.tradovate_observation import StreamHealth
from src.l3g_paper.policy import ExperimentalPaperPolicy
from tests.l3g_helpers import ObservationFactory


ROOT = Path(__file__).parents[1]
ADDON_SOURCE = (
    ROOT
    / "ninjatrader"
    / "NinjaScript"
    / "AddOns"
    / "BeelzebubReadOnlyAddOn.cs"
)
CONTRACT_BEGIN = "    // BEELZEBUB_PROVIDER_TIMESTAMP_CONTRACT_BEGIN"
CONTRACT_END = "    // BEELZEBUB_PROVIDER_TIMESTAMP_CONTRACT_END"


def _actual_csharp_contract() -> str:
    source = ADDON_SOURCE.read_text(encoding="utf-8")
    start = source.index(CONTRACT_BEGIN) + len(CONTRACT_BEGIN)
    end = source.index(CONTRACT_END, start)
    contract = source[start:end].strip()
    return (
        "using System;\n"
        "using System.Linq;\n"
        "namespace NinjaTrader.NinjaScript.AddOns\n"
        "{\n"
        f"{contract}\n"
        "}\n"
    )


def _execute_actual_csharp_contract() -> dict[str, dict[str, object]]:
    powershell = shutil.which("powershell.exe")
    if powershell is None:
        raise AssertionError("Windows PowerShell is required for the native C# timestamp contract test.")

    script = textwrap.dedent(
        r"""
        $ErrorActionPreference = 'Stop'
        Add-Type -Path $env:BEELZEBUB_PROVIDER_TIMESTAMP_CONTRACT

        function New-Unspecified([string] $Value) {
            $parsed = [DateTime]::ParseExact(
                $Value,
                'yyyy-MM-dd HH:mm:ss',
                [Globalization.CultureInfo]::InvariantCulture,
                [Globalization.DateTimeStyles]::None)
            return [DateTime]::SpecifyKind($parsed, [DateTimeKind]::Unspecified)
        }

        function New-Utc([string] $Value) {
            return [DateTime]::SpecifyKind((New-Unspecified $Value), [DateTimeKind]::Utc)
        }

        function Invoke-Conversion(
            [string] $Name,
            [DateTime] $Value,
            [TimeZoneInfo] $ApplicationZone,
            $Offset
        ) {
            [DateTime] $utc = [DateTime]::MinValue
            [string] $reason = $null
            [Nullable[TimeSpan]] $typedOffset = $null
            if ($null -ne $Offset) {
                $typedOffset = [TimeSpan] $Offset
            }
            $ok = [NinjaTrader.NinjaScript.AddOns.BeelzebubProviderTimestamp]::TryConvertUtc(
                $Value,
                $ApplicationZone,
                $typedOffset,
                [ref] $utc,
                [ref] $reason)
            return [ordered]@{
                name = $Name
                ok = $ok
                utc = $(if ($ok) {
                    $utc.ToString('o', [Globalization.CultureInfo]::InvariantCulture)
                } else {
                    $null
                })
                reason = $reason
                input_kind = $Value.Kind.ToString()
            }
        }

        $eastern = [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time')
        $applicationMismatch = [TimeZoneInfo]::CreateCustomTimeZone(
            'NINJATRADER_TEST_UTC_PLUS_09',
            [TimeSpan]::FromHours(9),
            'NinjaTrader test UTC+09',
            'NinjaTrader test UTC+09')
        $localOriginUtc = New-Utc '2026-02-15 18:30:00'
        $localValue = [TimeZoneInfo]::ConvertTimeFromUtc($localOriginUtc, [TimeZoneInfo]::Local)

        $cases = @(
            (Invoke-Conversion 'utc' (New-Utc '2026-07-15 16:00:00') $null $null),
            (Invoke-Conversion 'local' $localValue $applicationMismatch $null),
            (Invoke-Conversion 'application_mismatch' (New-Unspecified '2026-02-15 12:00:00') $applicationMismatch $null),
            (Invoke-Conversion 'app_winter' (New-Unspecified '2026-01-15 12:00:00') $eastern $null),
            (Invoke-Conversion 'app_summer' (New-Unspecified '2026-07-15 12:00:00') $eastern $null),
            (Invoke-Conversion 'dst_invalid' (New-Unspecified '2026-03-08 02:30:00') $eastern $null),
            (Invoke-Conversion 'dst_ambiguous' (New-Unspecified '2026-11-01 01:30:00') $eastern $null),
            (Invoke-Conversion 'dst_fold_daylight' (New-Unspecified '2026-11-01 01:30:00') $eastern ([TimeSpan]::FromHours(-4))),
            (Invoke-Conversion 'dst_fold_standard' (New-Unspecified '2026-11-01 01:30:00') $eastern ([TimeSpan]::FromHours(-5))),
            (Invoke-Conversion 'dst_fold_wrong_offset' (New-Unspecified '2026-11-01 01:30:00') $eastern ([TimeSpan]::FromHours(-6))),
            (Invoke-Conversion 'missing_application_zone' (New-Unspecified '2026-07-15 12:00:00') $null $null),
            (Invoke-Conversion 'unexpected_offset' (New-Unspecified '2026-07-15 12:00:00') $eastern ([TimeSpan]::FromHours(-4)))
        )

        [ordered]@{
            local_zone_id = [TimeZoneInfo]::Local.Id
            application_zone_id = $applicationMismatch.Id
            local_origin_utc = $localOriginUtc.ToString('o', [Globalization.CultureInfo]::InvariantCulture)
            cases = $cases
        } | ConvertTo-Json -Compress -Depth 5
        """
    )

    with tempfile.TemporaryDirectory() as directory:
        contract_path = Path(directory) / "BeelzebubProviderTimestamp.cs"
        contract_path.write_text(_actual_csharp_contract(), encoding="utf-8")
        environment = dict(os.environ)
        environment["BEELZEBUB_PROVIDER_TIMESTAMP_CONTRACT"] = str(contract_path)
        completed = subprocess.run(
            [powershell, "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script],
            cwd=ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    if completed.returncode != 0:
        raise AssertionError(
            "Actual C# timestamp contract failed to compile or execute:\n"
            + completed.stdout
            + completed.stderr
        )
    payload = json.loads(completed.stdout)
    return {
        "metadata": {
            "local_zone_id": payload["local_zone_id"],
            "application_zone_id": payload["application_zone_id"],
            "local_origin_utc": payload["local_origin_utc"],
        },
        **{case["name"]: case for case in payload["cases"]},
    }


class ProviderTimestampContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.results = _execute_actual_csharp_contract()

    def test_actual_csharp_contract_handles_kinds_zones_and_dst(self) -> None:
        results = self.results
        self.assertNotEqual(
            results["metadata"]["local_zone_id"],
            results["metadata"]["application_zone_id"],
        )
        self.assertEqual(results["utc"]["input_kind"], "Utc")
        self.assertEqual(results["utc"]["utc"], "2026-07-15T16:00:00.0000000Z")
        self.assertEqual(results["local"]["input_kind"], "Local")
        self.assertEqual(results["local"]["utc"], results["metadata"]["local_origin_utc"])
        self.assertEqual(results["application_mismatch"]["utc"], "2026-02-15T03:00:00.0000000Z")
        self.assertEqual(results["app_winter"]["utc"], "2026-01-15T17:00:00.0000000Z")
        self.assertEqual(results["app_summer"]["utc"], "2026-07-15T16:00:00.0000000Z")

        for name, reason in (
            ("dst_invalid", "PROVIDER_TIMESTAMP_DST_INVALID"),
            ("dst_ambiguous", "PROVIDER_TIMESTAMP_DST_AMBIGUOUS"),
            ("dst_fold_wrong_offset", "PROVIDER_TIMESTAMP_AMBIGUOUS_OFFSET_INVALID"),
            ("missing_application_zone", "PROVIDER_TIMESTAMP_APPLICATION_TIMEZONE_MISSING"),
            ("unexpected_offset", "PROVIDER_TIMESTAMP_OFFSET_NOT_APPLICABLE"),
        ):
            with self.subTest(name=name):
                self.assertFalse(results[name]["ok"])
                self.assertIsNone(results[name]["utc"])
                self.assertEqual(results[name]["reason"], reason)

        self.assertEqual(results["dst_fold_daylight"]["utc"], "2026-11-01T05:30:00.0000000Z")
        self.assertEqual(results["dst_fold_standard"]["utc"], "2026-11-01T06:30:00.0000000Z")

    def test_actual_csharp_timestamp_flows_into_stale_and_future_classification(self) -> None:
        provider_timestamp = str(self.results["app_summer"]["utc"])

        def classify(receipt_time: str) -> str:
            policy = ExperimentalPaperPolicy()
            policy.on_transport_state(StreamHealth.HEALTHY)
            factory = ObservationFactory(start=datetime.fromisoformat(receipt_time))
            observation = replace(factory.quote(100), provider_timestamp=provider_timestamp)
            return policy.ingest(observation).reason_code

        fresh = classify("2026-07-15T16:00:01+00:00")
        self.assertNotIn(fresh, {"STALE_EVENT_TIMESTAMP", "FUTURE_EVENT_TIMESTAMP"})
        self.assertEqual(classify("2026-07-15T16:01:31+00:00"), "STALE_EVENT_TIMESTAMP")
        self.assertEqual(classify("2026-07-15T15:59:58+00:00"), "FUTURE_EVENT_TIMESTAMP")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
