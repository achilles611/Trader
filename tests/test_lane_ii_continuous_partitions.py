from pathlib import Path
import tempfile
import unittest
from scripts.lane_ii_continuous.partitions import seal_partition


class PartitionTests(unittest.TestCase):
    def test_nonoverlap_and_partial_last_line_without_account_reset(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);capture=root/'capture';capture.mkdir();journal=capture/'events.jsonl'
            journal.write_bytes(b'{"n":1}\n{"n":')
            one=seal_partition(journal,root/'part1')
            self.assertEqual(one['complete_lines'],1)
            with journal.open('ab') as f:f.write(b'2}\n')
            two=seal_partition(journal,root/'part2',previous=one)
            self.assertEqual(two['start_offset'],one['end_offset'])
            self.assertEqual(Path(two['payload']['path']).read_bytes(),b'{"n":2}\n')
            self.assertFalse(two['account_reset'])

    def test_source_truncation_or_partition_corruption_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);capture=root/'capture';capture.mkdir();journal=capture/'events.jsonl'
            journal.write_text('{"n":1}\n')
            one=seal_partition(journal,root/'part1')
            journal.write_text('')
            with self.assertRaises(ValueError):seal_partition(journal,root/'part2',previous=one)

    def test_changed_prior_manifest_offsets_cannot_skip_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);capture=root/'capture';capture.mkdir();journal=capture/'events.jsonl'
            journal.write_text('{"n":1}\n');one=seal_partition(journal,root/'part1')
            one['end_offset']+=1
            with self.assertRaises(ValueError):seal_partition(journal,root/'part2',previous=one)

    def test_truncate_and_regrow_cannot_splice_a_new_journal(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);capture=root/'capture';capture.mkdir();journal=capture/'events.jsonl'
            journal.write_text('{"n":1}\n');one=seal_partition(journal,root/'part1')
            journal.write_text('{"n":2}\n{"n":3}\n')
            with self.assertRaises(ValueError):seal_partition(journal,root/'part2',previous=one)

    def test_prior_payload_corruption_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);capture=root/'capture';capture.mkdir();journal=capture/'events.jsonl'
            journal.write_text('{"n":1}\n');one=seal_partition(journal,root/'part1')
            Path(one['payload']['path']).write_text('{"n":2}\n')
            with self.assertRaises(ValueError):seal_partition(journal,root/'part2',previous=one)


if __name__=='__main__':unittest.main()
