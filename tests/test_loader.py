import unittest
from pathlib import Path
import pandas as pd

from NflDataLoader.dataloader import NflLoader

class TestNflLoader(unittest.TestCase):

    def test_get_game_eid(self):
        nloader = NflLoader()
        eid = nloader.get_game_eid("test", "test_sched", "test_team", update_schedule=False)
        self.assertEqual(eid, "test_eid")

    def test_get_game_stats(self):
        nloader = NflLoader()
        nloader.datapath = Path('tests/database')
        stats = nloader.get_game_stats("test_eid")
        self.assertEqual(stats['test_eid']['home']['abbr'], "test_team")
    
    def test_add_fpts(self):
        PATH = Path('tests/fixtures/test_fpts.csv')
        df = pd.read_csv(PATH)
        loader = NflLoader()
        df = loader.add_fpts(df)
        fpts_calculated = list(df['fpts'])
        fpts_soll = list(df['soll_fpts'])
        self.assertListEqual(fpts_calculated, fpts_soll)


if __name__ == "__main__":
    unittest.main()

