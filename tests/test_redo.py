import unittest
from aries import redo


class TestRedo(unittest.TestCase):
    def test_redo_all_losers(self):
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

        dp_table = {
            "P1": 10,
            "P2": 12,
        }

        disk_page = {
            "P1": {"pageLSN": 0, "value": 0},
            "P2": {"pageLSN": 0, "value": 0},
        }

        redone_lsns = redo(wal, dp_table, disk_page)

        self.assertEqual(redone_lsns, [10, 12])
        self.assertEqual(disk_page["P1"]["pageLSN"], 10)
        self.assertEqual(disk_page["P1"]["value"], 1)
        self.assertEqual(disk_page["P2"]["pageLSN"], 12)
        self.assertEqual(disk_page["P2"]["value"], 11)

    def test_redo_all_winners(self):
        # winners and losers are both redone...
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
            {"LSN": 5, "type": "COMMITTED", "tx": "T1"},
            {"LSN": 6, "type": "COMMITTED", "tx": "T2"},
        ]

        dp_table = {
            "P1": 10,
            "P2": 12,
        }

        disk_page = {
            "P1": {"pageLSN": 0, "value": 0},
            "P2": {"pageLSN": 0, "value": 0},
        }

        redone_lsns = redo(wal, dp_table, disk_page)
        self.assertEqual(redone_lsns, [10, 12])
        self.assertEqual(disk_page["P1"]["pageLSN"], 10)
        self.assertEqual(disk_page["P1"]["value"], 1)
        self.assertEqual(disk_page["P2"]["pageLSN"], 12)
        self.assertEqual(disk_page["P2"]["value"], 11)

    def test_doesnt_redo_if_page_lsn_after(self):
        # winners and losers are both redone...
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
            {"LSN": 20, "type": "COMMITTED", "tx": "T1"},
            {"LSN": 30, "type": "COMMITTED", "tx": "T2"},
        ]

        dp_table = {
            "P1": 10,
            "P2": 12,
        }

        disk_page = {
            "P1": {"pageLSN": 14, "value": 0},
            "P2": {"pageLSN": 16, "value": 0},
        }

        redone_lsns = redo(wal, dp_table, disk_page)
        self.assertEqual(redone_lsns, [])
        self.assertEqual(disk_page["P1"]["pageLSN"], 14)
        self.assertEqual(disk_page["P1"]["value"], 0)
        self.assertEqual(disk_page["P2"]["pageLSN"], 16)
        self.assertEqual(disk_page["P2"]["value"], 0)

    def test_skip_if_page_lsn_equals_log_lsn(self):
        wal = [{"LSN": 10, "type": "UPDATE", "tx": "T1", "page": "P1", "after": 1}]
        dp_table = {"P1": 5}

        disk_page = {"P1": {"pageLSN": 10, "value": 1}}

        redone_lsns = redo(wal, dp_table, disk_page)
        self.assertEqual(redone_lsns, [])

    def test_skip_if_page_not_in_dp_table(self):
        wal = [
            {"LSN": 10, "type": "UPDATE", "tx": "T1", "page": "P1", "after": 1},
        ]
        dp_table = {}
        disk_page = {"P1": {"pageLSN": 0, "value": 0}}

        redone_lsns = redo(wal, dp_table, disk_page)
        self.assertEqual(redone_lsns, [])


if __name__ == "__main__":
    unittest.main()
