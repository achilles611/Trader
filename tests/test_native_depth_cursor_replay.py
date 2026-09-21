from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
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
    ) -> None:
        self.bids = dict(enumerate(bids))
        self.asks = dict(enumerate(asks))
        self.last_sequence = last_sequence
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
            levels.pop(mutation.position, None)
            self._shift(levels, mutation.position, False)
        else:
            self.continuity_broken = True
            raise ValueError("oracle_operation_invalid")

        bids = self._prefix(self.bids, True)
        asks = self._prefix(self.asks, False)
        if not bids or not asks:
            return Publication(False, "NATIVE_DEPTH_POSITION_ZERO_UNAVAILABLE", (), ())
        if bids[0][0] >= asks[0][0]:
            return Publication(False, "CROSSED_NATIVE_POSITIONAL_SNAPSHOT", (), ())
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
        string reason;
        bool available = TryApplyDepthMutation(
            mutation, out bids, out asks, out bestBid, out bestAsk, out reason);
        return (available ? "1" : "0") + "\t" + (reason ?? "")
            + "\t" + bids + "\t" + asks
            + "\t" + EncodeWorking(nativeDepthWorkingBookBids)
            + "\t" + EncodeWorking(nativeDepthWorkingBookAsks);
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
                self.assertEqual(len(fields), 6, (case["journal"], mutation, output))
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
                publications.append(actual)

            self.assertTrue(publications[0].available, case["journal"])
            self.assertTrue(all(
                not publication.available for publication in publications[1:-1]
            ), case["journal"])
            self.assertTrue(publications[-1].available, case["journal"])
            self.assertEqual(mutations[-1].sequence, case["recovered_source"])
            self.assertEqual(
                case["recovered_journal"] - case["journal"],
                case["recovered_source"] - case["cross_sequence"],
            )

    def test_ask_side_mirror_never_reuses_pre_cross_cursor(self) -> None:
        initial_bids = levels(("100", 5), ("99.75", 4), ("99.5", 3))
        initial_asks = levels(("100.25", 6), ("100.5", 7), ("100.75", 8))
        oracle = IndependentNativeDepthOracle(initial_bids, initial_asks)
        self.assertTrue(oracle.apply(Mutation(1, "Remove", "Ask", 0)).available)
        for mutation in (
            Mutation(2, "Add", "Ask", 0, Decimal("99.75"), 2),
            Mutation(3, "Remove", "Ask", 1),
            Mutation(4, "Add", "Ask", 1, Decimal("100"), 3),
            Mutation(5, "Remove", "Ask", 0),
        ):
            publication = oracle.apply(mutation)
            self.assertFalse(publication.available)
            self.assertEqual(publication.reason, "CROSSED_NATIVE_POSITIONAL_SNAPSHOT")
        recovered = oracle.apply(Mutation(6, "Remove", "Ask", 0))
        self.assertTrue(recovered.available)
        self.assertEqual(recovered.asks, initial_asks[2:])

    def test_deeper_positions_and_zero_price_removes_are_authentic(self) -> None:
        oracle = IndependentNativeDepthOracle(
            levels(("100", 5), ("99.75", 4), ("99.25", 3)),
            levels(("100.25", 6), ("100.75", 8)),
        )
        inserted = oracle.apply(Mutation(1, "Add", "Bid", 2, Decimal("99.5"), 9))
        self.assertEqual(
            inserted.bids,
            levels(("100", 5), ("99.75", 4), ("99.5", 9), ("99.25", 3)),
        )
        updated = oracle.apply(Mutation(2, "Update", "Bid", 2, Decimal("99.5"), 10))
        self.assertEqual(updated.bids[2], (Decimal("99.5"), 10))
        removed = oracle.apply(Mutation(3, "Remove", "Bid", 2, Decimal("0"), 0))
        self.assertEqual(
            removed.bids,
            levels(("100", 5), ("99.75", 4), ("99.25", 3)),
        )

    def test_reset_rebuild_stays_unavailable_until_both_sides_are_coherent(self) -> None:
        oracle = IndependentNativeDepthOracle(
            levels(("100", 5)), levels(("100.25", 6))
        )
        bid_reset = oracle.apply(
            Mutation(1, "Add", "Bid", 0, Decimal("101"), 2, is_reset=True)
        )
        self.assertFalse(bid_reset.available)
        self.assertEqual(bid_reset.reason, "NATIVE_DEPTH_POSITION_ZERO_UNAVAILABLE")
        bid_rebuilt = oracle.apply(Mutation(2, "Add", "Ask", 0, Decimal("101.25"), 3))
        self.assertTrue(bid_rebuilt.available)

        ask_reset = oracle.apply(
            Mutation(3, "Add", "Ask", 0, Decimal("102"), 4, is_reset=True)
        )
        self.assertFalse(ask_reset.available)
        ask_rebuilt = oracle.apply(Mutation(4, "Add", "Bid", 0, Decimal("101.75"), 5))
        self.assertTrue(ask_rebuilt.available)

    def test_sequence_gap_latches_oracle_until_authoritative_rebuild(self) -> None:
        oracle = IndependentNativeDepthOracle(
            levels(("100", 5)), levels(("100.25", 6)), last_sequence=10
        )
        before = (dict(oracle.bids), dict(oracle.asks))
        with self.assertRaisesRegex(ValueError, "oracle_source_sequence_gap"):
            oracle.apply(Mutation(12, "Update", "Bid", 0, Decimal("100"), 7))
        self.assertTrue(oracle.continuity_broken)
        self.assertEqual((oracle.bids, oracle.asks), before)

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
