# loadstats.py
from pathlib import Path
from pprint import pprint
import pandas as pd

from NflDataLoader.dataloader import NflLoader, create_test_data
from NflDataLoader.scheduleloader import (
        ScheduleLoader, create_date_from_eid, save_obj_to_json
)
from NflDataLoader.roster import update_database

def main():
    season = 2019
    sl = ScheduleLoader(season, week=1, update=False)
    df = pd.DataFrame(sl.schedule)
    df['date'] = df.apply(lambda row: create_date_from_eid(row['eid']), axis=1)
    print(df.head())

def get_stats():
    seasons = (2016, 2017, 2018)
    for season in seasons:
        nl = NflLoader(season, save=False)
        _ = nl.get_seasontable()

def get_pre_season(season: int):
    loader = NflLoader(season, seasontype='PRE')
    stable = loader.get_seasontable()

"""
def dataloader():
    loader = NflLoader()
    t = loader.get_game_table(2018, 1, 'CAR', update_schedule=False, new=True)
    # print(t.head())
    cols = list(t.columns)
    filepath = Path('tests/fixtures')
    filename = 'table_columns.json'
    save_obj_to_json(cols, filepath, filename)
    t.to_csv("gametable.csv")
    tab = pd.read_csv("gametable.csv", index_col=0)
    print(tab.head())
"""

def load_player(playerid):
    from NflDataLoader.roster import download_player_data
    d = download_player_data(playerid)
    print(d)

def get_test_data():
    path = Path("NflDataLoader/database/test")
    path.mkdir(exist_ok=True)
    season = 2019
    weeks = [week for week in range(1, 18, 1)]
    data = create_test_data(season, weeks)
    data.to_csv(path / "test_data.csv")
    print(data.head())
    print(data.info)

def up_database():
    update_database()

if __name__ == "__main__":
    for season in (2018, 2017, 2016, 2015):
        get_pre_season(season)
        print(f"PreSeason {season} finished!")
    # get_test_data()
    # load_player("00-0031704")
