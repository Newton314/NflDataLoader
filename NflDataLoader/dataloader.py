import threading
from pathlib import Path
from typing import NewType

import requests
import pandas as pd
import numpy as np
from tqdm import tqdm

from .scheduleloader import (
    ScheduleLoader, load_json, save_obj_to_json, create_date_from_eid, add_dateinfo)
from .playerdataloader import get_player_infos, PlayerDataLoader

EID = NewType('EID', str)

class NflLoader():
    """Contains methods to download nfl game data.
    """
    def __init__(self, **kwargs):
        """
        optional arguments:
        new bool: if True creates new tables from scratch, ignoring old ones (default False)
        save bool: if True saves intermediate tables (default True)
        """
        self.new = kwargs.get('new', False)
        self.save = kwargs.get('save', True)
        self.schedule_loader = None
        self.tables = []
        self.weektables = {}
        self.datapath = Path("NflDataLoader/database")
        self.datapath.mkdir(exist_ok=True)
        self.schedule = None


    def get_game_eid(self, season: int, week: int, team: str, update_schedule=False) -> EID:
        '''
        return the eid for the given combination of season, week, team
        '''
        if (self.schedule_loader is None) or (self.schedule_loader.season != season):
            self.schedule_loader = ScheduleLoader(season, week, update=update_schedule)
        schedule = self.schedule_loader.get_schedule(season, week, seasontype='REG')
        for dic in schedule:
            if team in dic['home'] or team in dic['away']:
                return EID(dic['eid'])
        return None


    def get_game_stats(self, eid: EID) -> dict:
        '''
        eid -> dic(gamestats)
        '''
        jsonurl = f'http://www.nfl.com/liveupdate/game-center/{eid}/{eid}_gtd.json'
        directory_path = self.datapath / 'jsonarchive'
        filename = f'{eid}.json'
        filepath = directory_path / filename
        directory_path.mkdir(parents=True, exist_ok=True)
        try:
            return load_json(filepath)
        except FileNotFoundError:
            resp = requests.get(jsonurl, timeout=1)
            if resp.status_code == 200:
                save_obj_to_json(resp.json(), directory_path, filename)
                return resp.json()
            else:
                print("No Connection to game center")


    def __det_places(self, eid: EID, gamestats: dict, team: str) -> tuple:
        '''determines the home and away team
        -> (placeofteam, opponent, placeofopponent)
        '''
        if gamestats[eid]['home']['abbr'] == team:
            place = 'home'
            opponent = gamestats[eid]['away']['abbr']
            opp_place = 'away'
        else:
            place = 'away'
            opponent = gamestats[eid]['home']['abbr']
            opp_place = 'home'
        return (place, opponent, opp_place)


    def __adjust_exp(self, dframe, season):
        currentseason = self.schedule_loader.get_season()
        # breakpoint()
        if currentseason > season:
            dframe['exp'] = dframe['exp'].astype(int) - (currentseason - season)
            dframe['age'] = dframe['age'].astype(int) - (currentseason - season)
        return dframe


    def __create_subtable(self, dic: dict, category: str) -> pd.DataFrame:
        prefix = {'rushing': 'rush_', 'passing': 'pass_', 'receiving': 'recv_',
                  'kickret': 'kret_', 'puntret': 'pret_', 'kicking': 'k_', 'punting': 'p_'}
        dframe = pd.DataFrame(dic[category]).T
        dframe['player_id'] = dframe.index
        if category in prefix.keys():
            columns = list(dframe.columns)
            for column in columns:
                if column == 'name' or column == 'player_id':
                    continue
                columns[columns.index(column)] = prefix[category] + str(column)
                dframe[[column]] = dframe[[column]].astype(np.float64)
            dframe.columns = columns
        return dframe


    def __add_player_info(self, table: pd.DataFrame) -> pd.DataFrame:
        """adds infos about players to the given table"""
        player_ids = list(table['player_id'])
        playerinfos = get_player_infos(player_ids)
        try:
            del playerinfos['id']
            del playerinfos['status']
        except KeyError:
            pass
        return pd.merge(table, playerinfos, on='player_id', how='outer')


    def add_fpts(self, table):
        """new function to calculate fantasy points
        hopefuly faster than the old one
        """
        defense_positions = ('NT', 'DB', 'DT', 'LB', 'DE', 'CB', 'SAF')
        nr_defense_player = len(table[table['position'].isin(defense_positions)])
        table['fpts'] = 0
        table.fpts += table['pass_yds'] / 25 + table['pass_tds'] * 4 - table['pass_ints'] * 2
        table.fpts += table['rush_yds'] / 10 + table['rush_tds'] * 6
        table.fpts += table['recv_yds'] / 10 + table['recv_tds'] * 6
        table.fpts += (table['pass_twoptm'] + table['rush_twoptm'] + table['recv_twoptm']) * 2
        table.fpts += (table['rcv'] + table['trcv'] - table['lost']) * 2
        # vereinfachte Rechnung der punkte für fieldgoals
        # normally 5 pts for fg > 50 yds
        table.fpts += table['k_fgm'] * 3
        #continue here
        return table


    def add_standardfpts(self, table):
        """adds standard fantasypoints to the given table"""
        # punkte für safety und recovertouchdown fehlen noch!!
        # restrukturierung für bessere Performance und übersichtlichkeit!!!
        fpts = []
        defense = ['NT', 'DB', 'DT', 'LB', 'DE', 'CB', 'SAF']
        n_defplayer = len(table[table['position'].isin(defense)])
        # sfty = table.at[0, 'tot_score'] - table['pass_tds'].sum() * 6 - table['rush_tds'].sum()
        # * 6 - table['k_xpmade'].sum() - table['k_totpfg'].sum()
        for i in table.index:
            tab = table.loc[i]
            pts = 0
            pts += tab['pass_yds'] / 25 + tab['pass_tds'] * 4 - tab['pass_ints'] * 2
            pts += tab['rush_yds'] / 10 + tab['rush_tds'] * 6
            pts += tab['recv_yds'] / 10 + tab['recv_tds'] * 6
            pts += (tab['pass_twoptm'] + tab['rush_twoptm'] + tab['recv_twoptm']) * 2
            if 'rcv' in table.columns:
                pts += (tab['rcv'] + tab['trcv'] - tab['lost']) * 2
            if 'k_fgyds' in table.columns:
                if tab['k_fgyds'] >= 50:
                    fgfactor = 5
                else:
                    fgfactor = 3
                pts += tab['k_fgm'] * fgfactor
            if 'k_xpmade' in table.columns:
                pts += tab['k_xpmade']
            if 'pret_tds' in table.columns:
                pts += tab['pret_tds'] * 6
            if 'kret_tds' in table.columns:
                pts += tab['kret_tds'] * 6
            pts += tab['sk'] + tab['int'] * 2
            if tab['position'] in defense:
                pts_allwd = int(tab['pts_allwd'])
                if pts_allwd == 0:
                    pts += 10 / n_defplayer
                elif pts_allwd <= 6:
                    pts += 7 / n_defplayer
                elif pts_allwd <= 13:
                    pts += 4 / n_defplayer
                elif pts_allwd <= 20:
                    pts += 1 / n_defplayer
                elif pts_allwd <= 27:
                    pts += 0
                elif pts_allwd <= 34:
                    pts += -1 / n_defplayer
                else:
                    pts += -4 / n_defplayer
            fpts.append(pts)
        fpts = np.array(fpts)
        table['fpts'] = fpts
        return table


    def __create_game_table(self, season, week, team, update_schedule=True):
        table = pd.DataFrame()
        game_eid = self.get_game_eid(season, week, team, update_schedule=update_schedule)
        if game_eid is None:
            raise ValueError("No game eid available")
        gamestats = self.get_game_stats(game_eid)
        place, opponent, opp_place = self.__det_places(game_eid, gamestats, team)
        statistics = gamestats[game_eid][place]['stats']
        del statistics['team']
        categories = list(statistics.keys())
        categories.sort()
        for category in categories:
            if not table.empty:
                if statistics[category]:
                    table = pd.merge(table, self.__create_subtable(statistics, category),
                                     how='outer', on=['player_id', 'name'], sort=False)
            else:
                if statistics[category]:
                    table = self.__create_subtable(statistics, category)
        table['opponent'] = opponent
        if place == 'home':
            table['home'] = 1
            table['away'] = 0
        else:
            table['home'] = 0
            table['away'] = 1
        table['seasonweek'] = week
        table = add_dateinfo(table, create_date_from_eid(game_eid))
        table['tot_score'] = gamestats[game_eid][place]['score']['T']
        table['pts_allwd'] = gamestats[game_eid][opp_place]['score']['T']
        # table = table.sort_values(by='playerID')
        # table = table.reset_index(drop=True)
        del table['name']
        table = self.__add_player_info(table)
        table['team'] = team
        # breakpoint()
        table = self.__adjust_exp(table, season)
        table = table.fillna(value=0)
        table = self.add_standardfpts(table)
        if self.save:
            directory_path = self.datapath / str(season) / str(team)
            directory_path.mkdir(parents=True, exist_ok=True)
            table.to_csv(directory_path / f"{week}.csv")
        return table


    def get_game_table(self, season, week, team, update_schedule=True, **kwargs):
        self.new = kwargs.get('new', self.new)
        directory_path = self.datapath / str(season) / str(team)
        directory_path.mkdir(parents=True, exist_ok=True)
        file_path = directory_path / f"{week}.csv"
        if (file_path not in directory_path.iterdir()) or self.new:
            game_table = self.__create_game_table(
                season, week, team, update_schedule=update_schedule
                )
            return game_table
        return pd.read_csv(file_path, index_col=1)


    def __threading_job(self, season, week, team, **kwargs):
        table = self.get_game_table(season, week, team, **kwargs)
        with threading.Lock():
            self.tables.append(table)


    def __create_weektable(self, season, week,
                           seasontype='REG', update_schedule=True
                           ) -> pd.DataFrame:
        # gametables = []
        threads = []
        weektable = pd.DataFrame()
        if self.schedule_loader.season != season:
            self.schedule_loader = ScheduleLoader(
                season, week, seasontype='REG', update=update_schedule
                )
        self.schedule = pd.DataFrame(
            self.schedule_loader.get_schedule(season, week, seasontype=seasontype)
            )
        for i in self.schedule.index:
            for place in ('home', 'away'):
                team = self.schedule.at[i, place]
                args = (season, week, team)
                kwargs = {'update_schedule': update_schedule}
                thread = threading.Thread(target=self.__threading_job, args=args, kwargs=kwargs)
                threads.append(thread)
                thread.start()
        for thread in threads:
            thread.join()
        for table in self.tables:
            weektable = weektable.append(table, ignore_index=True, sort=False)
        if self.save:
            file_path = self.datapath / str(season) / f'{week}.csv'
            weektable.to_csv(file_path)
        self.weektables[str(week)] = weektable
        return weektable


    def get_weektable(self, season, week, seasontype='REG', update_schedule=True):
        directory_path = self.datapath / str(season)
        directory_path.mkdir(parents=True, exist_ok=True)
        file_path = directory_path / f'{week}.csv'
        if (file_path not in directory_path.iterdir()) or self.new:
            return self.__create_weektable(
                season, week, seasontype=seasontype, update_schedule=update_schedule
                )
        return self.weektables.get(str(week), pd.read_csv(file_path, index_col=0, parse_dates=True))


    def __create_seasontable(self, season, seasontype='REG', update_schedule=True):
        seasontable = pd.DataFrame()
        self.schedule_loader = ScheduleLoader(season, update=update_schedule)
        current_season = self.schedule_loader.get_season()
        if season < current_season:
            weeks = [x for x in range(1, 18, 1)]
            for week in tqdm(weeks, smoothing=0, desc="Weeks"):
                weektable = self.get_weektable(
                    season, week, seasontype=seasontype, update_schedule=False
                    )
                seasontable = seasontable.append(weektable, ignore_index=True, sort=False)
        if self.save:
            file_path = self.datapath / f'{season}.csv'
            seasontable.to_csv(file_path)
        return seasontable


    def get_seasontable(self, season, seasontype='REG'):
        file_path = self.datapath / f'{season}.csv'
        if (file_path not in self.datapath.iterdir()) or self.new:
            return self.__create_seasontable(season, seasontype=seasontype, update_schedule=True)
        return pd.read_csv(file_path, index_col=0, parse_dates=True)


def add_columns(df):
    df = df.copy()
    df["home"] = 0
    df["away"] = 0
    df["opponent"] = None
    df["date"] = None
    df["year"] = 0
    df["month"] = 0
    df["day"] = 0
    df["weekday"] = 0
    return df

def create_test_data(season: int, weeks: list) -> pd.DataFrame:
    """create a DataFrame for predictions"""
    week = weeks[0]
    schedule_loader = ScheduleLoader(season=season, week=week, update=False)
    player_loader = PlayerDataLoader()
    schedule = schedule_loader.schedule
    schedule_frame = pd.DataFrame(schedule)
    print(schedule_frame.head())
    # get active players
    active_players = pd.DataFrame(player_loader.get_active_players())
    # get active teams
    active_teams = list(schedule_frame['home']) + list(schedule_frame['away'])
    test_data = active_players[active_players.team.isin(active_teams)].copy()
    test_data = add_columns(test_data)
    for row in schedule_frame.iterrows():
        row = row[1]
        home_view = test_data[test_data['team'] == row["home"]].index
        test_data.loc[home_view, "home"] = 1
        test_data.loc[home_view, "away"] = 0
        test_data.loc[home_view, "opponent"] = row["away"]
        gamedate = create_date_from_eid(row["eid"])
        test_data.loc[home_view, "date"] = gamedate
        test_data.loc[home_view, "year"] = gamedate.year
        test_data.loc[home_view, "month"] = gamedate.month
        test_data.loc[home_view, "day"] = gamedate.day
        test_data.loc[home_view, "weekday"] = gamedate.weekday()

        away_view = test_data[test_data['team'] == row["away"]].index
        test_data.loc[away_view, "home"] = 0
        test_data.loc[away_view, "away"] = 1
        test_data.loc[away_view, "opponent"] = row["home"]
        test_data.loc[away_view, "date"] = gamedate
        test_data.loc[away_view, "year"] = gamedate.year
        test_data.loc[away_view, "month"] = gamedate.month
        test_data.loc[away_view, "day"] = gamedate.day
        test_data.loc[away_view, "weekday"] = gamedate.weekday()
    test_data['seasonweek'] = week
    del test_data['id']
    del test_data["status"]
    # del test_data["esb_id"]
    return test_data
    # continue here
    # raise NotImplementedError

if __name__ == "__main__":
    create_test_data(2019, [1,])
