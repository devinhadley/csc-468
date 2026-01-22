import unittest
from aries import redo


class TestRedo(unittest.TestCase):
    def test_redo_all_losers(self) -> none:
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


if __name__ == "__main__":
    unittest.main()
