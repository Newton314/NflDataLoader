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


if __name__ == "__main__":
    main()
