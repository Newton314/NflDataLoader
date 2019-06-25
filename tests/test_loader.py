import unittest
from pathlib import Path
import pandas as pd

from NflDataLoader.dataloader import NflLoader, create_test_data
from NflDataLoader.scheduleloader import load_json

class TestDataLoader(unittest.TestCase):

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

    def test_create_table(self):
        PATH = Path('tests/fixtures/table_columns.json')
        columns = load_json(PATH)
        loader = NflLoader()
        season, week, team = (2018, 1, 'CAR')
        table = loader.get_game_table(season, week, team, update_schedule=False, new=True)
        self.assertListEqual(columns, list(table.columns))

    def test_create_test_data(self):
        PATH = Path("tests/fixtures/test_columns.json")
        columns = load_json(PATH)
        week = [10,]
        table = create_test_data(season=2019, weeks=week)
        test_columns = list(table.columns)
        columns.sort()
        test_columns.sort()
        self.assertEqual(len(columns), len(test_columns))
        self.assertListEqual(columns, test_columns)


if __name__ == "__main__":
    unittest.main()
