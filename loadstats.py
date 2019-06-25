# loadstats.py
from pathlib import Path
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
    nl = NflLoader(save=False)
    for season in seasons:
        _ = nl.get_seasontable(season)


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

def get_test_data():
    season = 2019
    weeks = [1, 2]
    data = create_test_data(season, weeks)
    data.to_csv("test_data.csv")
    print(data.head())
    print(data.columns)

def set_database():
    update_database()

if __name__ == "__main__":
    #update_database()
    get_test_data()
