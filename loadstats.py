# loadstats.py
import pandas as pd

from NflDataLoader.dataloader import NflLoader
from NflDataLoader.scheduleloader import ScheduleLoader, create_date_from_eid


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


if __name__ == "__main__":
    # main()
    get_stats()
