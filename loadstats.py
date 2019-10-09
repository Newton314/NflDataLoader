# loadstats.py
from pathlib import Path
import pandas as pd

from NflDataLoader.dataloader import NflLoader, create_test_data
from NflDataLoader.scheduleloader import (
        ScheduleLoader, create_date_from_eid
)
from NflDataLoader.roster import update_database, download_roster
from NflDataLoader.active_players import get_active_players_for_all_teams, get_injured_players

def main():
    season = 2019
    sl = ScheduleLoader(season, week=1, update=False)
    df = pd.DataFrame(sl.schedule)
    df['date'] = df.apply(lambda row: create_date_from_eid(row['eid']), axis=1)
    print(df.head())

def get_players():
    p = get_active_players_for_all_teams()
    p.to_csv("actives.csv")
    print(p.head())

def get_stats():
    seasons = (2014, 2015, 2016)
    for season in seasons:
        nl = NflLoader(season, save=True)
        _ = nl.get_seasontable()

def get_stats_from_current_season(weeks: list):
    season = 2019
    loader = NflLoader(season)
    weekdata = []
    for week in weeks:
        weekdata.append(loader.get_weektable(week))
    df = pd.concat(weekdata, sort=False)
    df.to_csv(loader.datapath.parent/f"{season}_REG.csv")
    print(df.head())

def get_pre_season(season: int):
    loader = NflLoader(season, seasontype='PRE')
    stable = loader.get_seasontable()
    return stable

def load_player(playerid):
    from NflDataLoader.roster import download_player_data
    d = download_player_data(playerid)
    print(d)

def get_test_data():
    # up_database()
    path = Path("NflDataLoader/database/test")
    path.mkdir(exist_ok=True)
    season = 2019
    start = 6
    delta = 3
    end = min(start+delta, 17)
    weeks = [week for week in range(start, end, 1)]
    data = create_test_data(season, weeks)
    data.to_csv(path / "test_data.csv")

def up_database():
    update_database()

def get_roster():
    team = 'ARI'
    roster = pd.DataFrame(download_roster(team))
    roster.to_csv("houston_roster.csv")
    print(roster.head(10))

def get_depth():
    from NflDataLoader.active_players import get_depth_info
    team = 'CAR'
    depthframe = get_depth_info(team)
    print(depthframe.head())
    return depthframe

def injured():
    inj = get_injured_players("ARI")
    print(inj)

if __name__ == "__main__":
    # injured()
    # get_stats_from_current_season((1, 2, 3, 4, 5))
    get_test_data()
    # get_roster()
    # load_player("00-0031704")
    # get_stats()
    # get_players()
    # df = get_depth()
