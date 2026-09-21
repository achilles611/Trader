from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


Level = tuple[Decimal, int]


@dataclass(frozen=True)
class Mutation:
    sequence: int
    operation: str
    side: str
    position: int
    price: Decimal = Decimal("0")
    volume: int = 0
    is_reset: bool = False
    generation: int = 1


@dataclass(frozen=True)
class Publication:
    available: bool
    reason: str | None
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]


class IndependentNativeDepthOracle:
    """Independent positional-book oracle; no production helper is imported."""

    def __init__(
        self,
        bids: tuple[Level, ...] = (),
        asks: tuple[Level, ...] = (),
        *,
        last_sequence: int = 0,
        generation: int = 1,
    ) -> None:
        self.bids = dict(enumerate(bids))
        self.asks = dict(enumerate(asks))
        self.last_sequence = last_sequence
        self.generation = generation
        self.continuity_broken = False

    @staticmethod
    def _shift(levels: dict[int, Level], position: int, inserting: bool) -> None:
        positions = [
            current
            for current in levels
            if (current >= position if inserting else current > position)
        ]
        positions.sort(reverse=inserting)
        for current in positions:
            levels[current + 1 if inserting else current - 1] = levels.pop(current)

    @staticmethod
    def _prefix(levels: dict[int, Level], descending: bool) -> tuple[Level, ...]:
        result: list[Level] = []
        for position in range(10):
            row = levels.get(position)
            if row is None or row[0] <= 0 or row[1] <= 0:
                break
            if result:
                prior_price = result[-1][0]
                if (descending and prior_price <= row[0]) or (
                    not descending and prior_price >= row[0]
                ):
                    break
            result.append(row)
        return tuple(result)

    def apply(self, mutation: Mutation) -> Publication:
        if self.continuity_broken:
            raise ValueError("oracle_continuity_broken")
        if mutation.generation != self.generation:
            self.continuity_broken = True
            raise ValueError("oracle_source_generation_changed")
        if mutation.sequence != self.last_sequence + 1:
            self.continuity_broken = True
            raise ValueError("oracle_source_sequence_gap")
        self.last_sequence = mutation.sequence
        if mutation.side not in {"Bid", "Ask"}:
            self.continuity_broken = True
            raise ValueError("oracle_side_invalid")
        if mutation.is_reset:
            self.bids.clear()
            self.asks.clear()
        levels = self.bids if mutation.side == "Bid" else self.asks
        remove_identity_unresolved = False
        if mutation.operation == "Add":
            if mutation.position < 0 or mutation.price <= 0 or mutation.volume <= 0:
                self.continuity_broken = True
                raise ValueError("oracle_add_invalid")
            self._shift(levels, mutation.position, True)
            levels[mutation.position] = (mutation.price, mutation.volume)
        elif mutation.operation == "Update":
            if mutation.position < 0 or mutation.price <= 0 or mutation.volume <= 0:
                self.continuity_broken = True
                raise ValueError("oracle_update_invalid")
            levels[mutation.position] = (mutation.price, mutation.volume)
        elif mutation.operation == "Remove":
            if mutation.position < 0:
                self.continuity_broken = True
                raise ValueError("oracle_remove_invalid")
            remove_identity_unresolved = mutation.position not in levels
            levels.pop(mutation.position, None)
            self._shift(levels, mutation.position, False)
        else:
            self.continuity_broken = True
            raise ValueError("oracle_operation_invalid")

        bids = self._prefix(self.bids, True)
        asks = self._prefix(self.asks, False)
        if remove_identity_unresolved:
            return Publication(
                False, "NATIVE_DEPTH_REMOVE_IDENTITY_UNRESOLVED", bids, asks
            )
        if not bids or not asks:
            return Publication(False, "NATIVE_DEPTH_POSITION_ZERO_UNAVAILABLE", bids, asks)
        if bids[0][0] >= asks[0][0]:
            return Publication(False, "CROSSED_NATIVE_POSITIONAL_SNAPSHOT", bids, asks)
        if len(bids) != 10 or len(asks) != 10:
            return Publication(False, "NATIVE_DEPTH_EXACT_10X10_REQUIRED", bids, asks)
        return Publication(True, None, bids, asks)


def levels(*rows: tuple[str, int]) -> tuple[Level, ...]:
    return tuple((Decimal(price), volume) for price, volume in rows)


def captured_mutations(case: dict[str, object]) -> tuple[Mutation, ...]:
    """Expand the compact authentic seam rows without consulting production code."""
    cross_sequence = int(case["cross_sequence"])
    rows = [Mutation(cross_sequence - 1, "Remove", "Bid", 0)]
    cross = case["cross"]
    rows.append(
        Mutation(cross_sequence, "Add", "Bid", 0, Decimal(cross[0]), cross[1])
    )
    sequence = cross_sequence + 1
    for position, (price, volume) in enumerate(case["bid_rebuild"], start=1):
        rows.append(Mutation(sequence, "Remove", "Bid", position))
        rows.append(Mutation(sequence + 1, "Add", "Bid", position, price, volume))
        sequence += 2
    for position, price, volume in case["ask_rebuild"]:
        rows.append(Mutation(sequence, "Remove", "Ask", 0))
        rows.append(Mutation(sequence + 1, "Add", "Ask", position, price, volume))
        sequence += 2
    rows.append(Mutation(sequence, "Remove", "Ask", 0))
    return tuple(rows)


def extract_production_depth_methods(source: str) -> str:
    boundaries = (
        ("        private static bool FinitePositive(double value)",
         "        private bool TryApplyDepthMutation("),
        ("        private bool TryApplyDepthMutation(",
         "        private void CommitNativeDepthWorkingState("),
        ("        private void CommitNativeDepthWorkingState(",
         "        private static void ShiftKnownPositions("),
        ("        private static void ShiftKnownPositions(",
         "        private static List<KeyValuePair<double, long>> DepthPrefix("),
        ("        private static List<KeyValuePair<double, long>> DepthPrefix(",
         "        public bool NeedsRecovery("),
    )
    return "\n".join(source[source.index(start):source.index(end)] for start, end in boundaries)


def build_production_depth_harness(directory: Path) -> Path:
    addon = (
        Path(__file__).parents[1]
        / "ninjatrader/NinjaScript/AddOns/BeelzebubReadOnlyAddOn.cs"
    ).read_text(encoding="utf-8")
    methods = extract_production_depth_methods(addon)
    prefix = r'''
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

public enum Operation { Add, Update, Remove }
public enum MarketDataType { Bid, Ask, Last }
public sealed class MarketDepthEventArgs
{
    public Operation Operation;
    public MarketDataType MarketDataType;
    public int Position;
    public double Price;
    public long Volume;
    public bool IsReset;
}

public sealed class ProductionDepthHarness
{
    private const int MaximumPublishedBookLevelsPerSide = 10;
    private readonly SortedDictionary<int, KeyValuePair<double, long>> nativeDepthWorkingBookBids =
        new SortedDictionary<int, KeyValuePair<double, long>>();
    private readonly SortedDictionary<int, KeyValuePair<double, long>> nativeDepthWorkingBookAsks =
        new SortedDictionary<int, KeyValuePair<double, long>>();
    private bool depthContinuityBroken;
    private bool depthPositionDisclosurePending;

    public void Seed(string sideName, string encoded)
    {
        SortedDictionary<int, KeyValuePair<double, long>> side = sideName == "Bid"
            ? nativeDepthWorkingBookBids : nativeDepthWorkingBookAsks;
        side.Clear();
        string[] rows = encoded.Split(new char[] {';'}, StringSplitOptions.RemoveEmptyEntries);
        for (int position = 0; position < rows.Length; position++)
        {
            string[] fields = rows[position].Split(':');
            side[position] = new KeyValuePair<double, long>(
                Double.Parse(fields[0], CultureInfo.InvariantCulture),
                Int64.Parse(fields[1], CultureInfo.InvariantCulture));
        }
    }

    public string Apply(string encoded)
    {
        string[] fields = encoded.Split('\t');
        MarketDepthEventArgs mutation = new MarketDepthEventArgs();
        mutation.Operation = (Operation)Enum.Parse(typeof(Operation), fields[2]);
        mutation.MarketDataType = (MarketDataType)Enum.Parse(typeof(MarketDataType), fields[3]);
        mutation.Position = Int32.Parse(fields[4], CultureInfo.InvariantCulture);
        mutation.Price = Double.Parse(fields[5], CultureInfo.InvariantCulture);
        mutation.Volume = Int64.Parse(fields[6], CultureInfo.InvariantCulture);
        mutation.IsReset = Boolean.Parse(fields[7]);
        string bids;
        string asks;
        double bestBid;
        double bestAsk;
        double resolvedPrice;
        string bidPositions;
        string askPositions;
        bool positionCursorDisclosed;
        string reason;
        bool available = TryApplyDepthMutation(
            mutation, out bids, out asks, out bidPositions, out askPositions,
            out bestBid, out bestAsk, out resolvedPrice,
            out positionCursorDisclosed, out reason);
        return (available ? "1" : "0") + "\t" + (reason ?? "")
            + "\t" + bids + "\t" + asks
            + "\t" + EncodeWorking(nativeDepthWorkingBookBids)
            + "\t" + EncodeWorking(nativeDepthWorkingBookAsks)
            + "\t" + (positionCursorDisclosed ? "1" : "0")
            + "\t" + bidPositions + "\t" + askPositions
            + "\t" + resolvedPrice.ToString("R", CultureInfo.InvariantCulture);
    }

    private static string EncodeWorking(
        SortedDictionary<int, KeyValuePair<double, long>> side)
    {
        return String.Join(";", side.Select(row =>
            row.Key.ToString(CultureInfo.InvariantCulture) + ":"
            + row.Value.Key.ToString("R", CultureInfo.InvariantCulture) + ":"
            + row.Value.Value.ToString(CultureInfo.InvariantCulture)));
    }
'''
    suffix = r'''
}

public static class Program
{
    public static void Main()
    {
        ProductionDepthHarness harness = new ProductionDepthHarness();
        string line;
        while ((line = Console.ReadLine()) != null)
        {
            if (line.StartsWith("SEED\t", StringComparison.Ordinal))
            {
                string[] fields = line.Split('\t');
                harness.Seed(fields[1], fields[2]);
                Console.WriteLine("SEEDED");
            }
            else
                Console.WriteLine(harness.Apply(line));
        }
    }
}
'''
    source_path = directory / "ProductionDepthHarness.cs"
    executable_path = directory / "ProductionDepthHarness.exe"
    source_path.write_text(prefix + methods + suffix, encoding="utf-8")
    compiler = Path(r"C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe")
    if not compiler.exists():
        compiler = Path(r"C:\Windows\Microsoft.NET\Framework\v4.0.30319\csc.exe")
    if not compiler.exists():
        raise unittest.SkipTest(".NET Framework C# compiler is unavailable")
    result = subprocess.run(
        [str(compiler), "/nologo", "/target:exe", f"/out:{executable_path}", str(source_path)],
        cwd=directory,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            "Production depth harness failed to compile:\n"
            + result.stdout + "\n" + result.stderr
        )
    return executable_path


def encode_seed(rows: tuple[Level, ...]) -> str:
    return ";".join(f"{price}:{volume}" for price, volume in rows)


def encode_mutation(mutation: Mutation) -> str:
    return "\t".join((
        "MUT", str(mutation.sequence), mutation.operation, mutation.side,
        str(mutation.position), str(mutation.price), str(mutation.volume),
        str(mutation.is_reset),
    ))


def decode_levels(encoded: str) -> tuple[Level, ...]:
    return tuple(
        (Decimal(str(row["price"])), int(row["size"]))
        for row in json.loads(encoded)
    )


def decode_working(encoded: str) -> dict[int, Level]:
    if not encoded:
        return {}
    result: dict[int, Level] = {}
    for row in encoded.split(";"):
        position, price, volume = row.split(":")
        result[int(position)] = (Decimal(price), int(volume))
    return result


def decode_positions(encoded: str) -> dict[int, Level]:
    return {
        int(row["position"]): (Decimal(str(row["price"])), int(row["size"]))
        for row in json.loads(encoded)
    }


CAPTURED_BID_SEAMS = (
    {
        "journal": 87603,
        "cross_sequence": 186798,
        "bids": levels(
            ("30111", 3), ("30110.75", 10), ("30110.5", 14),
            ("30110.25", 12), ("30110", 11), ("30109.75", 13),
            ("30109.5", 8), ("30109.25", 20), ("30109", 16),
            ("30108.75", 31),
        ),
        "asks": levels(
            ("30111.25", 2), ("30111.5", 6), ("30111.75", 8),
            ("30112", 7), ("30112.25", 7), ("30112.5", 9),
            ("30112.75", 8), ("30113", 20), ("30113.25", 22),
            ("30113.5", 12),
        ),
        "cross": ("30111.5", 1),
        "second": ("30111.25", 11),
        "bid_rebuild": levels(
            ("30111.25", 11), ("30111", 10), ("30110.75", 11),
            ("30110.5", 14), ("30110.25", 16), ("30110", 14),
            ("30109.75", 16), ("30109.5", 8), ("30109.25", 20),
            ("30109", 16), ("30108.75", 31), ("30108.5", 11),
            ("30108.25", 37), ("30108", 45), ("30107.75", 22),
            ("30107.5", 13), ("30107.25", 19), ("30107", 23),
            ("30106.75", 13), ("30106.5", 8), ("30106.25", 12),
            ("30106", 12), ("30105.75", 10), ("30105.5", 14),
            ("30105.25", 8), ("30105", 20), ("30104.75", 9),
            ("30104.5", 10), ("30104.25", 20),
        ),
        "ask_rebuild": ((2, Decimal("30111.75"), 3),),
        "recovered_source": 186859,
        "recovered_journal": 87664,
    },
    {
        "journal": 92264,
        "cross_sequence": 191291,
        "bids": levels(
            ("30109.75", 4), ("30109.5", 7), ("30109.25", 13),
            ("30109", 12), ("30108.75", 27), ("30108.5", 6),
            ("30108.25", 36), ("30108", 45), ("30107.75", 21),
            ("30107.5", 13),
        ),
        "asks": levels(
            ("30110.25", 4), ("30110.5", 8), ("30110.75", 11),
            ("30111", 13), ("30111.25", 9), ("30111.5", 8),
            ("30111.75", 10), ("30112", 12), ("30112.25", 10),
            ("30112.5", 10),
        ),
        "cross": ("30110.5", 1),
        "second": ("30110.25", 8),
        "bid_rebuild": levels(
            ("30110.25", 8), ("30110", 9), ("30109.75", 8),
            ("30109.5", 9), ("30109.25", 13), ("30109", 20),
            ("30108.75", 28), ("30108.5", 9), ("30108.25", 36),
            ("30108", 45), ("30107.75", 21), ("30107.5", 13),
            ("30107.25", 19), ("30107", 23), ("30106.75", 13),
            ("30106.5", 8), ("30106.25", 12), ("30106", 12),
            ("30105.75", 10), ("30105.5", 15), ("30105.25", 8),
            ("30105", 20), ("30104.75", 9), ("30104.5", 10),
            ("30104.25", 21), ("30104", 19), ("30103.75", 15),
            ("30103.5", 8), ("30103.25", 14),
        ),
        "ask_rebuild": ((2, Decimal("30110.75"), 2),),
        "recovered_source": 191352,
        "recovered_journal": 92325,
    },
    {
        "journal": 93189,
        "cross_sequence": 192014,
        "bids": levels(
            ("30110.5", 6), ("30110.25", 9), ("30110", 14),
            ("30109.75", 9), ("30109.5", 9), ("30109.25", 13),
            ("30109", 15), ("30108.75", 28), ("30108.5", 11),
            ("30108.25", 38),
        ),
        "asks": levels(
            ("30111", 2), ("30111.25", 8), ("30111.5", 9),
            ("30111.75", 6), ("30112", 8), ("30112.25", 11),
            ("30112.5", 11), ("30112.75", 22), ("30113", 23),
            ("30113.25", 17),
        ),
        "cross": ("30111.75", 2),
        "second": ("30111.5", 7),
        "bid_rebuild": levels(
            ("30111.5", 7), ("30111.25", 9), ("30111", 9),
            ("30110.75", 10), ("30110.5", 10), ("30110.25", 12),
            ("30110", 17), ("30109.75", 12), ("30109.5", 12),
            ("30109.25", 15), ("30109", 15), ("30108.75", 28),
            ("30108.5", 11), ("30108.25", 38), ("30108", 45),
            ("30107.75", 21), ("30107.5", 13), ("30107.25", 19),
            ("30107", 28), ("30106.75", 13), ("30106.5", 8),
            ("30106.25", 12), ("30106", 12), ("30105.75", 12),
            ("30105.5", 15), ("30105.25", 8), ("30105", 21),
            ("30104.75", 9), ("30104.5", 10),
        ),
        "ask_rebuild": (
            (4, Decimal("30112"), 3),
            (5, Decimal("30112.25"), 9),
            (6, Decimal("30112.5"), 10),
        ),
        "recovered_source": 192079,
        "recovered_journal": 93254,
    },
    {
        "journal": 93678,
        "cross_sequence": 192469,
        "bids": levels(
            ("30111.5", 4), ("30111.25", 6), ("30111", 11),
            ("30110.75", 10), ("30110.5", 10), ("30110.25", 8),
            ("30110", 22), ("30109.75", 12), ("30109.5", 13),
            ("30109.25", 17),
        ),
        "asks": levels(
            ("30112", 7), ("30112.25", 9), ("30112.5", 11),
            ("30112.75", 9), ("30113", 10), ("30113.25", 13),
            ("30113.5", 11), ("30113.75", 24), ("30114", 16),
            ("30114.25", 15),
        ),
        "cross": ("30112.25", 5),
        "second": ("30112", 5),
        "bid_rebuild": levels(
            ("30112", 5), ("30111.75", 10), ("30111.5", 10),
            ("30111.25", 8), ("30111", 13), ("30110.75", 13),
            ("30110.5", 13), ("30110.25", 12), ("30110", 23),
            ("30109.75", 12), ("30109.5", 12), ("30109.25", 17),
            ("30109", 19), ("30108.75", 29), ("30108.5", 11),
            ("30108.25", 38), ("30108", 45), ("30107.75", 21),
            ("30107.5", 13), ("30107.25", 19), ("30107", 28),
            ("30106.75", 13), ("30106.5", 8), ("30106.25", 12),
            ("30106", 12), ("30105.75", 12), ("30105.5", 15),
            ("30105.25", 8), ("30105", 21),
        ),
        "ask_rebuild": ((2, Decimal("30112.5"), 3),),
        "recovered_source": 192530,
        "recovered_journal": 93739,
    },
    {
        "journal": 95691,
        "cross_sequence": 194409,
        "bids": levels(
            ("30111.25", 10), ("30111", 10), ("30110.75", 10),
            ("30110.5", 10), ("30110.25", 8), ("30110", 17),
            ("30109.75", 12), ("30109.5", 14), ("30109.25", 17),
            ("30109", 20),
        ),
        "asks": levels(
            ("30111.75", 3), ("30112", 6), ("30112.25", 11),
            ("30112.5", 11), ("30112.75", 9), ("30113", 10),
            ("30113.25", 11), ("30113.5", 12), ("30113.75", 26),
            ("30114", 26),
        ),
        "cross": ("30112", 1),
        "second": ("30111.75", 6),
        "bid_rebuild": levels(
            ("30111.75", 6), ("30111.5", 11), ("30111.25", 10),
            ("30111", 10), ("30110.75", 10), ("30110.5", 10),
            ("30110.25", 12), ("30110", 17), ("30109.75", 12),
            ("30109.5", 14), ("30109.25", 17), ("30109", 20),
            ("30108.75", 29), ("30108.5", 11), ("30108.25", 38),
            ("30108", 45), ("30107.75", 21), ("30107.5", 14),
            ("30107.25", 19), ("30107", 28), ("30106.75", 13),
            ("30106.5", 8), ("30106.25", 12), ("30106", 12),
            ("30105.75", 12), ("30105.5", 15), ("30105.25", 8),
            ("30105", 21), ("30104.75", 9),
        ),
        "ask_rebuild": ((2, Decimal("30112.25"), 1),),
        "recovered_source": 194470,
        "recovered_journal": 95752,
    },
)


# Compact fixture derived from, but not copying, the preserved authenticated
# 149,996,841-byte journal.  The whole-file identity and per-record chain/wire
# identities make the journal-47 regression traceable to the immutable source.
AUTHENTIC_JOURNAL_47_PROVENANCE = {
    "path": (
        r"N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs"
        r"\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collectors"
        r"\pong-collector-20260921T043228465785Z-0fefc92586ff"
        r"\authenticated-observations.jsonl"
    ),
    "bytes": 149_996_841,
    "sha256": "361d9f5a612090475c4fa7a2a7a72d1fb6fb54defb537895dc58957d81f36087",
    "source_generation": 1,
    "records": (
        (46, 955866, "2d9ed27bca2331a14f6cd5ad1762a502f2ec00560fe04235e447e859154e89b9", "e3df38c290f9ba13c8787fe72d1b021edf8ed5b2279699bba970074e30bf5163"),
        (47, 955867, "9b311649f701c7968437f94e15b61cb3320d3f7ab54fc4fa23f87974f643441d", "f3aedfffc2eedd3f7cb02b4c3f5144646543ac5683fbfddc3eb34128fbc9495c"),
        (48, 955868, "f60a94d15cfa9490c3a59d8b05c0e659fb2a57d7fa2ddfaaa9d7beae4a522d51", "dbe8b649cfc2cf9da31c9998cffb9270ce4b0e92f950282e5646c088c8c7995f"),
        (49, 955869, "56b60de02894bc4dccde067aaa70745ca4e2a71177af960c8172a69cb1e21484", "dc6e8b8aa6e101c703d5a79e1a0599f1f5c5e9b7a0571a411326ddeb524327d5"),
        (50, 955870, "06f5a6cf94fa441caa9f38780d003df78ece17a16dd1143fe92f45af448ecb46", "010aa50c8a73db42777e278cfe08445622001c9321e7065263d5f4befbbb5edb"),
        (51, 955871, "11d77d364d279e6722028ee880fdc92070f2c12fe39dbc443282dc0ea88fd972", "260fb38f7eb757f3c4c3b44095d664f267d0948ce3d0a193fd3d3c0c3f6efea1"),
        (52, 955872, "db50e9462ac3c5bc1ab83b81393d2e4c7aae9761bb1d4159a683007acbf05e30", "f3ffc2206a3ef4c81abc84114840b91f7270f26ddb0412cb3e97575e4b060f48"),
    ),
}
AUTHENTIC_JOURNAL_47_FIXTURE_SHA256 = "647be4f516a1a9c58a144b43e140be1d47dd7a0728f8d914081982ffc05a8837"
AUTHENTIC_JOURNAL_47_BIDS = levels(
    ("30096.5", 5), ("30096.25", 9), ("30096", 8),
    ("30095.75", 15), ("30095.5", 13), ("30095.25", 11),
    ("30095", 16), ("30094.75", 28), ("30094.5", 24),
    ("30094.25", 15),
)
AUTHENTIC_JOURNAL_46_ASKS = levels(
    ("30097", 9), ("30097.25", 14), ("30097.5", 12),
    ("30097.75", 11), ("30098", 14), ("30098.25", 15),
    ("30098.5", 15), ("30098.75", 17), ("30099", 14),
    ("30099.25", 16),
)
AUTHENTIC_JOURNAL_47_NEIGHBOR_MUTATIONS = (
    Mutation(955867, "Add", "Ask", 1, Decimal("30097"), 7),
    Mutation(955868, "Remove", "Ask", 0),
    Mutation(955869, "Add", "Ask", 2, Decimal("30097.25"), 14),
    Mutation(955870, "Remove", "Ask", 1),
    Mutation(955871, "Add", "Ask", 3, Decimal("30097.5"), 12),
    Mutation(955872, "Remove", "Ask", 2),
)


def authentic_journal_47_fixture_material() -> bytes:
    value = {
        "provenance": AUTHENTIC_JOURNAL_47_PROVENANCE,
        "bids": [(str(price), size) for price, size in AUTHENTIC_JOURNAL_47_BIDS],
        "asks": [(str(price), size) for price, size in AUTHENTIC_JOURNAL_46_ASKS],
        "mutations": [
            (
                item.sequence, item.operation, item.side, item.position,
                str(item.price), item.volume, item.is_reset, item.generation,
            )
            for item in AUTHENTIC_JOURNAL_47_NEIGHBOR_MUTATIONS
        ],
    }
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


class NativeDepthCursorReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._harness_directory = tempfile.TemporaryDirectory(
            prefix="beelzebub-native-depth-replay-"
        )
        cls._harness = build_production_depth_harness(
            Path(cls._harness_directory.name)
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls._harness_directory.cleanup()

    def _run_production(
        self,
        bids: tuple[Level, ...],
        asks: tuple[Level, ...],
        mutations: tuple[Mutation, ...],
    ) -> list[list[str]]:
        executed = subprocess.run(
            [str(self._harness)],
            input="\n".join((
                "SEED\tBid\t" + encode_seed(bids),
                "SEED\tAsk\t" + encode_seed(asks),
                *(encode_mutation(item) for item in mutations),
            )) + "\n",
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(executed.returncode, 0, executed.stderr)
        rows = executed.stdout.splitlines()
        self.assertEqual(rows[:2], ["SEEDED", "SEEDED"])
        return [row.split("\t") for row in rows[2:]]

    def test_all_five_captured_seams_match_executed_production_and_oracle(self) -> None:
        self.assertEqual(
            [(case["journal"], case["cross_sequence"]) for case in CAPTURED_BID_SEAMS],
            [
                (87603, 186798),
                (92264, 191291),
                (93189, 192014),
                (93678, 192469),
                (95691, 194409),
            ],
        )
        for case in CAPTURED_BID_SEAMS:
            cross_sequence = case["cross_sequence"]
            mutations = captured_mutations(case)
            self.assertEqual(
                [mutation.sequence for mutation in mutations],
                list(range(cross_sequence - 1, case["recovered_source"] + 1)),
                case["journal"],
            )
            self.assertEqual(
                (mutations[0].operation, mutations[0].side, mutations[0].position),
                ("Remove", "Bid", 0),
            )
            self.assertEqual(
                (mutations[1].operation, mutations[1].side, mutations[1].position),
                ("Add", "Bid", 0),
            )
            self.assertEqual(
                (mutations[2].operation, mutations[2].side, mutations[2].position),
                ("Remove", "Bid", 1),
            )
            self.assertEqual(
                (mutations[3].operation, mutations[3].side, mutations[3].position),
                ("Add", "Bid", 1),
            )
            oracle = IndependentNativeDepthOracle(
                case["bids"], case["asks"], last_sequence=cross_sequence - 2
            )
            input_text = "\n".join((
                "SEED\tBid\t" + encode_seed(case["bids"]),
                "SEED\tAsk\t" + encode_seed(case["asks"]),
                *(encode_mutation(mutation) for mutation in mutations),
            )) + "\n"
            executed = subprocess.run(
                [str(self._harness)], input=input_text, text=True,
                capture_output=True, check=False,
            )
            self.assertEqual(executed.returncode, 0, executed.stderr)
            output_rows = executed.stdout.splitlines()
            self.assertEqual(output_rows[:2], ["SEEDED", "SEEDED"])
            self.assertEqual(len(output_rows[2:]), len(mutations))

            publications: list[Publication] = []
            for mutation, output in zip(mutations, output_rows[2:], strict=True):
                expected = oracle.apply(mutation)
                fields = output.split("\t")
                self.assertEqual(len(fields), 10, (case["journal"], mutation, output))
                actual = Publication(
                    fields[0] == "1", fields[1] or None,
                    decode_levels(fields[2]), decode_levels(fields[3]),
                )
                self.assertEqual(actual, expected, (case["journal"], mutation.sequence))
                self.assertEqual(
                    decode_working(fields[4]), oracle.bids,
                    (case["journal"], mutation.sequence, "bids"),
                )
                self.assertEqual(
                    decode_working(fields[5]), oracle.asks,
                    (case["journal"], mutation.sequence, "asks"),
                )
                disclosed = fields[6] == "1"
                expected_disclosure = (
                    not expected.available
                    or len(expected.bids) < 10
                    or len(expected.asks) < 10
                    or mutation is mutations[-1]
                )
                self.assertEqual(
                    disclosed, expected_disclosure,
                    (case["journal"], mutation.sequence, "disclosure"),
                )
                if disclosed:
                    self.assertEqual(decode_positions(fields[7]), oracle.bids)
                    self.assertEqual(decode_positions(fields[8]), oracle.asks)
                publications.append(actual)

            self.assertFalse(publications[0].available, case["journal"])
            self.assertEqual(
                publications[0].reason,
                "NATIVE_DEPTH_EXACT_10X10_REQUIRED",
                case["journal"],
            )
            self.assertTrue(all(
                not publication.available for publication in publications[1:-1]
            ), case["journal"])
            self.assertFalse(publications[-1].available, case["journal"])
            self.assertEqual(
                publications[-1].reason,
                "NATIVE_DEPTH_EXACT_10X10_REQUIRED",
                case["journal"],
            )
            self.assertEqual(mutations[-1].sequence, case["recovered_source"])
            self.assertEqual(
                case["recovered_journal"] - case["journal"],
                case["recovered_source"] - case["cross_sequence"],
            )

    def test_disclosure_covers_seam_reconciliation_once_and_resolves_remove_price(self) -> None:
        bids = levels(*((str(100 - Decimal(index) / 4), index + 1) for index in range(10)))
        asks = levels(*((str(Decimal("100.25") + Decimal(index) / 4), index + 1) for index in range(10)))
        mutations = (
            Mutation(1, "Add", "Bid", 0, Decimal("100.25"), 7),
            Mutation(2, "Remove", "Bid", 0, Decimal("0"), 0),
            Mutation(3, "Update", "Bid", 0, Decimal("100"), 9),
        )
        executed = subprocess.run(
            [str(self._harness)],
            input="\n".join((
                "SEED\tBid\t" + encode_seed(bids),
                "SEED\tAsk\t" + encode_seed(asks),
                *(encode_mutation(item) for item in mutations),
            )) + "\n",
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(executed.returncode, 0, executed.stderr)
        rows = [line.split("\t") for line in executed.stdout.splitlines()[2:]]
        self.assertEqual([row[6] for row in rows], ["1", "1", "0"])
        self.assertEqual(rows[0][0:2], ["0", "CROSSED_NATIVE_POSITIONAL_SNAPSHOT"])
        self.assertEqual(rows[1][0:2], ["1", ""])
        self.assertEqual(rows[2][0:2], ["1", ""])
        self.assertEqual(Decimal(rows[1][9]), Decimal("100.25"))
        self.assertEqual(decode_positions(rows[0][7])[0], (Decimal("100.25"), 7))
        self.assertEqual(decode_positions(rows[1][7]), decode_working(rows[1][4]))

    def test_authenticated_journal_47_and_neighbors_are_truthfully_unavailable(self) -> None:
        fixture_hash = hashlib.sha256(authentic_journal_47_fixture_material()).hexdigest()
        self.assertEqual(fixture_hash, AUTHENTIC_JOURNAL_47_FIXTURE_SHA256)
        self.assertEqual(
            [row[:2] for row in AUTHENTIC_JOURNAL_47_PROVENANCE["records"]],
            [(46, 955866), (47, 955867), (48, 955868), (49, 955869),
             (50, 955870), (51, 955871), (52, 955872)],
        )
        rows = self._run_production(
            AUTHENTIC_JOURNAL_47_BIDS,
            AUTHENTIC_JOURNAL_46_ASKS,
            AUTHENTIC_JOURNAL_47_NEIGHBOR_MUTATIONS,
        )
        oracle = IndependentNativeDepthOracle(
            AUTHENTIC_JOURNAL_47_BIDS,
            AUTHENTIC_JOURNAL_46_ASKS,
            last_sequence=955866,
        )
        expected = [oracle.apply(item) for item in AUTHENTIC_JOURNAL_47_NEIGHBOR_MUTATIONS]
        actual = [
            Publication(row[0] == "1", row[1] or None,
                        decode_levels(row[2]), decode_levels(row[3]))
            for row in rows
        ]
        self.assertEqual(actual, expected)
        self.assertEqual(
            [(item.available, item.reason, len(item.asks)) for item in actual],
            [
                (False, "NATIVE_DEPTH_EXACT_10X10_REQUIRED", 1),
                (True, None, 10),
                (False, "NATIVE_DEPTH_EXACT_10X10_REQUIRED", 2),
                (True, None, 10),
                (False, "NATIVE_DEPTH_EXACT_10X10_REQUIRED", 3),
                (True, None, 10),
            ],
        )

    def test_mirrored_10x1_and_1x10_are_unavailable_in_executed_production(self) -> None:
        bids = levels(*((str(100 - Decimal(index) / 4), index + 1) for index in range(10)))
        asks = levels(*((str(Decimal("100.25") + Decimal(index) / 4), index + 1) for index in range(10)))
        ten_by_one = self._run_production(
            bids, asks[:1],
            (Mutation(1, "Update", "Ask", 0, asks[0][0], asks[0][1] + 1),),
        )[0]
        one_by_ten = self._run_production(
            bids[:1], asks,
            (Mutation(1, "Update", "Bid", 0, bids[0][0], bids[0][1] + 1),),
        )[0]
        for row, shape in ((ten_by_one, (10, 1)), (one_by_ten, (1, 10))):
            self.assertEqual(row[:2], ["0", "NATIVE_DEPTH_EXACT_10X10_REQUIRED"])
            self.assertEqual((len(decode_levels(row[2])), len(decode_levels(row[3]))), shape)

    def test_partial_and_crossed_books_recover_only_at_exact_coherent_10x10(self) -> None:
        bids = levels(*((str(100 - Decimal(index) / 4), index + 1) for index in range(10)))
        asks = levels(*((str(Decimal("100.25") + Decimal(index) / 4), index + 1) for index in range(10)))
        partial_rows = self._run_production(
            bids, asks[:9],
            (
                Mutation(1, "Update", "Ask", 0, Decimal("100.25"), 99),
                Mutation(2, "Add", "Ask", 9, Decimal("102.5"), 10),
            ),
        )
        self.assertEqual(partial_rows[0][:2], ["0", "NATIVE_DEPTH_EXACT_10X10_REQUIRED"])
        self.assertEqual(partial_rows[1][:2], ["1", ""])

        crossed_rows = self._run_production(
            bids, asks,
            (
                Mutation(1, "Add", "Bid", 0, Decimal("100.25"), 7),
                Mutation(2, "Remove", "Bid", 0),
                Mutation(3, "Update", "Ask", 0, Decimal("100.25"), 8),
            ),
        )
        self.assertEqual(crossed_rows[0][:2], ["0", "CROSSED_NATIVE_POSITIONAL_SNAPSHOT"])
        self.assertEqual(crossed_rows[1][:2], ["1", ""])
        self.assertEqual(crossed_rows[2][:2], ["1", ""])

    def test_known_and_unknown_zero_price_removes_preserve_identity_rules(self) -> None:
        bids = levels(*((str(100 - Decimal(index) / 4), index + 1) for index in range(11)))
        asks = levels(*((str(Decimal("100.25") + Decimal(index) / 4), index + 1) for index in range(10)))
        known = self._run_production(
            bids, asks, (Mutation(1, "Remove", "Bid", 10),)
        )[0]
        self.assertEqual(known[:2], ["1", ""])
        self.assertEqual(Decimal(known[9]), bids[10][0])

        unknown_rows = self._run_production(
            bids[:10], asks,
            (
                Mutation(1, "Remove", "Bid", 15),
                Mutation(2, "Update", "Bid", 0, Decimal("100"), 22),
            ),
        )
        self.assertEqual(
            unknown_rows[0][:2],
            ["0", "NATIVE_DEPTH_REMOVE_IDENTITY_UNRESOLVED"],
        )
        self.assertEqual(unknown_rows[0][9], "NaN")
        self.assertEqual(unknown_rows[1][:2], ["1", ""])

    def test_reset_and_sparse_rebuild_stay_unavailable_until_exact_10x10(self) -> None:
        bids = levels(*((str(100 - Decimal(index) / 4), index + 1) for index in range(10)))
        asks = levels(*((str(Decimal("100.25") + Decimal(index) / 4), index + 1) for index in range(10)))
        mutations = [Mutation(1, "Add", "Bid", 0, Decimal("100"), 1, is_reset=True)]
        sequence = 2
        for position in range(1, 10):
            mutations.append(Mutation(
                sequence, "Add", "Bid", position,
                Decimal("100") - Decimal(position) / 4, position + 1,
            ))
            sequence += 1
        # Position five first proves a sparse-prefix state is unavailable.
        mutations.append(Mutation(sequence, "Update", "Ask", 5, Decimal("101.5"), 6))
        sequence += 1
        for position in range(10):
            if position == 5:
                continue
            mutations.append(Mutation(
                sequence, "Update", "Ask", position,
                Decimal("100.25") + Decimal(position) / 4, position + 1,
            ))
            sequence += 1
        rows = self._run_production(bids, asks, tuple(mutations))
        self.assertTrue(all(row[0] == "0" for row in rows[:-1]))
        self.assertEqual(rows[-1][:2], ["1", ""])

    def test_sequence_gap_latches_oracle_until_authoritative_rebuild(self) -> None:
        oracle = IndependentNativeDepthOracle(
            levels(("100", 5)), levels(("100.25", 6)), last_sequence=10
        )
        before = (dict(oracle.bids), dict(oracle.asks))
        with self.assertRaisesRegex(ValueError, "oracle_source_sequence_gap"):
            oracle.apply(Mutation(12, "Update", "Bid", 0, Decimal("100"), 7))
        self.assertTrue(oracle.continuity_broken)
        self.assertEqual((oracle.bids, oracle.asks), before)

        replacement = IndependentNativeDepthOracle(
            levels(*((str(100 - Decimal(index) / 4), index + 1) for index in range(10))),
            levels(*((str(Decimal("100.25") + Decimal(index) / 4), index + 1) for index in range(10))),
            generation=2,
        )
        self.assertTrue(
            replacement.apply(Mutation(
                1, "Update", "Bid", 0, Decimal("100"), 8, generation=2
            )).available
        )
        with self.assertRaisesRegex(ValueError, "oracle_source_generation_changed"):
            replacement.apply(Mutation(
                2, "Update", "Bid", 0, Decimal("100"), 9, generation=1
            ))

    def test_invalid_native_side_is_terminal_in_executed_production(self) -> None:
        bids = levels(*((str(100 - Decimal(index) / 4), index + 1) for index in range(10)))
        asks = levels(*((str(Decimal("100.25") + Decimal(index) / 4), index + 1) for index in range(10)))
        rows = self._run_production(
            bids, asks,
            (
                Mutation(1, "Update", "Last", 0, Decimal("100"), 1),
                Mutation(2, "Update", "Bid", 0, Decimal("100"), 2),
            ),
        )
        self.assertEqual(rows[0][:2], ["0", "NATIVE_DEPTH_SIDE_INVALID"])
        self.assertEqual(rows[1][:2], ["0", "NATIVE_DEPTH_CONTINUITY_BROKEN"])

    def test_production_commits_working_cursor_before_publishability_checks(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "ninjatrader/NinjaScript/AddOns/BeelzebubReadOnlyAddOn.cs"
        ).read_text(encoding="utf-8")
        mutation = source[
            source.index("        private bool TryApplyDepthMutation("):
            source.index("        private static void ShiftKnownPositions(")
        ]
        commit = mutation.index(
            "CommitNativeDepthWorkingState(candidateBids, candidateAsks);"
        )
        self.assertLess(commit, mutation.index("DepthPrefix(candidateBids, true)"))
        self.assertLess(commit, mutation.index("if (bidPrefix.Count == 0 || askPrefix.Count == 0)"))
        self.assertLess(commit, mutation.index("if (bestNativeBid >= bestNativeAsk)"))
        self.assertEqual(mutation.count("CommitNativeDepthWorkingState("), 2)
        self.assertNotIn("CommitDepthState", mutation)
        self.assertIn("depthContinuityBroken = true", mutation)
        self.assertIn('failureReason = "NATIVE_DEPTH_CONTINUITY_BROKEN";', mutation)


if __name__ == "__main__":
    unittest.main()
