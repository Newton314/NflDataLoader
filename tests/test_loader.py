import unittest

from datetime import date

from NflDataLoader.dataloader import NflLoader

class TestNflLoader(unittest.TestCase):
    
    def test_get_game_eid(self):
        nloader = NflLoader()
        eid = nloader.get_game_eid("test", "test_sched", "test_team", update_schedule=False)
        self.assertEqual(eid, "test_eid")
    
    def test_get_game_stats(self):
        nloader = NflLoader()
        stats = nloader.get_game_stats("test_eid")
        self.assertEqual(stats['test_eid']['home']['abbr'], "test_team")
    
    def test_create_date_from_eid(self):
        nloader = NflLoader()
        year = 1992
        month = 8
        day = 15
        d = date(year, month, day)
        eid = f"{year}0{month}{day}"
        self.assertEqual(nloader.create_date_from_eid(eid), d)


if __name__ == "__main__":
    unittest.main()

