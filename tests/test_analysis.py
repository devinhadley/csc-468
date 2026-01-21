import unittest

from aries import analysis


class TestAnalysis(unittest.TestCase):
    def test_simple_no_checkpoint(self) -> None:
        wal = [
            {"LSN": 5, "type": "BEGIN", "tx": "T1"},
            {"LSN": 6, "type": "BEGIN", "tx": "T2"},
            {
                "LSN": 10,
                "type": "UPDATE",
                "tx": "T1",
                "page": "P1",
                "before": 0,
                "after": 1,
            },
            {
                "LSN": 12,
                "type": "UPDATE",
                "tx": "T2",
                "page": "P2",
                "before": 10,
                "after": 11,
            },
        ]

        tt, dpt = analysis(wal)

        self.assertIn("T1", tt)
        self.assertIn("T2", tt)
        self.assertEqual(tt["T1"]["status"], "RUNNING")
        self.assertEqual(tt["T2"]["status"], "RUNNING")
        self.assertEqual(tt["T1"]["lastLSN"], 10)
        self.assertEqual(tt["T2"]["lastLSN"], 12)
        self.assertEqual(dpt["P1"], 10)
        self.assertEqual(dpt["P2"], 12)

    def test_one_winner_one_loser(self) -> None:
        wal = [
            {"LSN": 5, "type": "BEGIN", "tx": "T1"},
            {"LSN": 6, "type": "BEGIN", "tx": "T2"},
            {
                "LSN": 10,
                "type": "UPDATE",
                "tx": "T1",
                "page": "P1",
                "before": 0,
                "after": 1,
            },
            {
                "LSN": 12,
                "type": "UPDATE",
                "tx": "T2",
                "page": "P2",
                "before": 10,
                "after": 11,
            },
            {"LSN": 15, "type": "COMMIT", "tx": "T1"},
        ]

        tt, dpt = analysis(wal)

        self.assertEqual(tt["T1"]["status"], "COMMITTED")
        self.assertEqual(tt["T2"]["status"], "RUNNING")

    def test_end_removes_from_tt(self) -> None:
        wal = [
            {"LSN": 5, "type": "BEGIN", "tx": "T1"},
            {
                "LSN": 10,
                "type": "UPDATE",
                "tx": "T1",
                "page": "P1",
                "before": 0,
                "after": 1,
            },
            {"LSN": 15, "type": "COMMIT", "tx": "T1"},
            {"LSN": 20, "type": "END", "tx": "T1"},
        ]

        tt, dpt = analysis(wal)

        self.assertNotIn("T1", tt)

    def test_dpt_keeps_earliest_reclsn(self) -> None:
        wal = [
            {"LSN": 5, "type": "BEGIN", "tx": "T1"},
            {
                "LSN": 10,
                "type": "UPDATE",
                "tx": "T1",
                "page": "P1",
                "before": 0,
                "after": 1,
            },
            {
                "LSN": 15,
                "type": "UPDATE",
                "tx": "T1",
                "page": "P1",
                "before": 1,
                "after": 2,
            },
            {
                "LSN": 20,
                "type": "UPDATE",
                "tx": "T1",
                "page": "P1",
                "before": 2,
                "after": 3,
            },
        ]

        tt, dpt = analysis(wal)

        self.assertEqual(dpt["P1"], 10)


if __name__ == "__main__":
    unittest.main()
