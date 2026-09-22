import unittest

from tools.pong_receiver_comparison import scheduled_completion_age_report


class ScheduledCompletionAgeReportTests(unittest.TestCase):
    def test_reports_each_record_against_its_scheduled_release(self) -> None:
        report = scheduled_completion_age_report(
            [100.125, 100.350, 100.775],
            [0.0, 0.2, 0.5],
            100.0,
        )

        self.assertTrue(report["complete"])
        self.assertEqual(report["count"], 3)
        self.assertEqual(report["expected_count"], 3)
        self.assertAlmostEqual(report["maximum_milliseconds"], 275.0)

    def test_marks_a_partial_receiver_result_incomplete(self) -> None:
        report = scheduled_completion_age_report([10.1], [0.0, 0.2], 10.0)

        self.assertFalse(report["complete"])
        self.assertEqual(report["count"], 1)
        self.assertEqual(report["expected_count"], 2)


if __name__ == "__main__":
    unittest.main()
