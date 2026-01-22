import unittest
from aries import undo


class TestUndo(unittest.TestCase):
    def test_undo_all_losers(self):
        wal = [
            {"LSN": 5, "type": "BEGIN", "tx": "T1"},
            {"LSN": 6, "type": "BEGIN", "tx": "T2"},
            {
                "LSN": 10,
                "type": "UPDATE",
                "tx": "T1",
                "page": "P1",
                "before": 1,
                "after": 1,
            },
            {
                "LSN": 12,
                "type": "UPDATE",
                "tx": "T2",
                "page": "P2",
                "before": 2,
                "after": 11,
            },
        ]

        original_wal_len = len(wal)

        transaction_table = {
            "T1": {"status": "RUNNING", "lastLSN": 10},
            "T2": {"status": "RUNNING", "lastLSN": 12},
        }

        disk_page = {
            "P1": {"pageLSN": 10, "value": 1},
            "P2": {"pageLSN": 12, "value": 11},
        }

        undone_lsns = undo(wal, transaction_table, disk_page)

        self.assertEqual(undone_lsns, [12, 10])
        self.assertEqual(disk_page["P1"]["pageLSN"], 14)
        self.assertEqual(disk_page["P1"]["value"], 1)
        self.assertEqual(disk_page["P2"]["pageLSN"], 13)
        self.assertEqual(disk_page["P2"]["value"], 2)
        self.assertTrue(len(wal) > original_wal_len)  # clrs were added...

    def test_undo_all_winners(self):
        wal = [
            {"LSN": 5, "type": "BEGIN", "tx": "T1"},
            {"LSN": 6, "type": "BEGIN", "tx": "T2"},
            {
                "LSN": 10,
                "type": "UPDATE",
                "tx": "T1",
                "page": "P1",
                "before": 0,
                "after": 10,
            },
            {
                "LSN": 20,
                "type": "UPDATE",
                "tx": "T2",
                "page": "P2",
                "before": 0,
                "after": 20,
            },
            {"LSN": 30, "type": "COMMIT", "tx": "T1"},
            {"LSN": 40, "type": "COMMIT", "tx": "T2"},
            {"LSN": 60, "type": "END", "tx": "T2"},
        ]

        transaction_table = {
            "T1": {"status": "COMMITTED", "lastLSN": 30},
        }

        disk_page = {
            "P1": {"pageLSN": 10, "value": 10},
            "P2": {"pageLSN": 20, "value": 20},
        }

        original_wal_len = len(wal)

        undone_lsns = undo(wal, transaction_table, disk_page)

        self.assertEqual(undone_lsns, [])

        self.assertEqual(disk_page["P1"]["value"], 10)
        self.assertEqual(disk_page["P1"]["pageLSN"], 10)

        self.assertEqual(disk_page["P2"]["value"], 20)
        self.assertEqual(disk_page["P2"]["pageLSN"], 20)

        self.assertEqual(len(wal), original_wal_len)  # no clrs

    def test_undo_mixed_winners_losers(self):
        wal = [
            {"LSN": 5, "type": "BEGIN", "tx": "T1"},
            {"LSN": 6, "type": "BEGIN", "tx": "T2"},
            {
                "LSN": 10,
                "type": "UPDATE",
                "tx": "T1",
                "page": "P1",
                "before": 0,
                "after": 100,
            },
            {
                "LSN": 20,
                "type": "UPDATE",
                "tx": "T2",
                "page": "P2",
                "before": 0,
                "after": 200,
            },
            {"LSN": 30, "type": "COMMIT", "tx": "T1"},
            {
                "LSN": 40,
                "type": "UPDATE",
                "tx": "T2",
                "page": "P3",
                "before": 0,
                "after": 300,
            },
        ]

        transaction_table = {
            "T1": {"status": "COMMITTED", "lastLSN": 30},
            "T2": {"status": "RUNNING", "lastLSN": 40},
        }

        disk_page = {
            "P1": {"pageLSN": 10, "value": 100},
            "P2": {"pageLSN": 20, "value": 200},
            "P3": {"pageLSN": 40, "value": 300},
        }

        undone_lsns = undo(wal, transaction_table, disk_page)

        self.assertEqual(undone_lsns, [40, 20])

        self.assertEqual(disk_page["P1"]["value"], 100)
        self.assertEqual(disk_page["P1"]["pageLSN"], 10)

        self.assertEqual(disk_page["P3"]["value"], 0)
        self.assertEqual(disk_page["P3"]["pageLSN"], 41)

        self.assertEqual(disk_page["P2"]["value"], 0)
        self.assertEqual(disk_page["P2"]["pageLSN"], 42)

        self.assertEqual(len(wal), 8)
