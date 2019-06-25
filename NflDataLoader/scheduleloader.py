import io
import shelve
import json
from xml.sax import make_parser
from datetime import date
from pathlib import Path
from typing import NewType

import requests
from requests.exceptions import Timeout
import pandas as pd

from .NFLHandler import NflHandler

EID = NewType('EID', str)

class ScheduleLoader():
    def __init__(self, season, week=None, seasontype='REG', update=True, **kwargs):
        """
        opt argument:
        path: str = location for the schedule data
        """
        self.season = season
        self.seasontype = seasontype
        self.base_path = Path(kwargs.get('path', 'NflDataLoader/database/schedule'))
        self.directory_path = self.base_path / str(season)
        self.directory_path.mkdir(parents=True, exist_ok=True)
        if update:
            self.update_schedule()
        if week:
            self.week = week
            self.schedule = self.get_schedule(self.season, self.week, self.seasontype)
        else:
            self.week = None
            self.schedule = None


    def __load_schedule(self, season, week, seasontype):
        '''
        loads game schedule for the given combination of season & week,
        returns list of game dictionaries,
        '''
        url = 'http://www.nfl.com/ajax/scorestrip'
        payload = {'season': season, 'seasonType': seasontype, 'week': week}
        try:
            schedule = requests.get(url, payload, timeout=2)
        except Timeout:
            print(f"Timeout during schedule connection! Season: {season}, Week: {week}")
            return None
        if schedule.status_code == 200:
            schedule.encoding = 'UTF-8'
            schedule_file = io.StringIO(schedule.text)
            parser = make_parser()
            content_handler = NflHandler(season, seasontype, week)
            parser.setContentHandler(content_handler)
            parser.parse(schedule_file)
            return content_handler.get_games()
        print("Couldn't load schedule")
        return None


    def update_schedule(self):
        '''
        updates the game schedule for the given season
        '''
        for week in range(1, 18, 1):
            schedule = self.__load_schedule(self.season, week, self.seasontype)
            save_obj_to_json(schedule, self.directory_path, f"{week}.json")
        return


    def get_schedule(self, season, week, seasontype):
        '''
        returns the games for a given combination of season, week, seasontype
        '''
        self.directory_path = self.base_path / str(season)
        filename = f"{week}.json"
        file_path = self.directory_path / filename
        if file_path in self.directory_path.iterdir():
            schedule = load_json(file_path)
        else:
            schedule = self.__load_schedule(season, week, seasontype)
            save_obj_to_json(schedule, self.directory_path, filename)
        return schedule


    def get_season(self, dte=date.today()):
        '''
        calculates the season for the given datetime.date
        (standard is the current date),
        a season is named after the year it started,
        breakpoint for a new season is 01.03.
        '''
        if dte.month < 3:
            season = dte.year - 1
        else:
            season = dte.year
        return season


    def get_current_week(self, dte=date.today()):
        '''
        needs rework
        '''
        season = self.get_season(dte=dte)
        shelf = shelve.open(f'database/schedule/shelves/{season}')
        for week in range(1, 18, 1):
            games = shelf[str(week)]
            open_games = 0
            for game in games:
                if not game['finished']:
                    open_games += 1
            if open_games > 0:
                shelf.close()
                return week
        shelf.close()
        return 17


def save_obj_to_json(obj, fpath, filename):
    fpath.mkdir(parents=True, exist_ok=True)
    filepath = fpath / filename
    with open(filepath, 'w') as jfile:
        json.dump(obj, jfile, sort_keys=True, indent=4)


def load_json(filepath):
    with open(filepath, 'r') as jfile:
        obj = json.load(jfile)
    return obj


def create_date_from_eid(eid: EID) -> date:
    year = int(eid[:4])
    month = int(eid[4:6])
    day = int(eid[6:8])
    return date(year, month, day)


def add_dateinfo(dframe: pd.DataFrame, dte: date) -> pd.DataFrame:
    dframe['date'] = dte
    dframe['year'] = dte.year
    dframe['month'] = dte.month
    dframe['day'] = dte.day
    dframe['weekday'] = dte.weekday()
    return dframe

TEAMS = [
    ['ARI', 'Arizona', 'Cardinals', 'Arizona Cardinals'],
    ['ATL', 'Atlanta', 'Falcons', 'Atlanta Falcons'],
    ['BAL', 'Baltimore', 'Ravens', 'Baltimore Ravens'],
    ['BUF', 'Buffalo', 'Bills', 'Buffalo Bills'],
    ['CAR', 'Carolina', 'Panthers', 'Carolina Panthers'],
    ['CHI', 'Chicago', 'Bears', 'Chicago Bears'],
    ['CIN', 'Cincinnati', 'Bengals', 'Cincinnati Bengals'],
    ['CLE', 'Cleveland', 'Browns', 'Cleveland Browns'],
    ['DAL', 'Dallas', 'Cowboys', 'Dallas Cowboys'],
    ['DEN', 'Denver', 'Broncos', 'Denver Broncos'],
    ['DET', 'Detroit', 'Lions', 'Detroit Lions'],
    ['GB', 'Green Bay', 'Packers', 'Green Bay Packers'],
    ['HOU', 'Houston', 'Texans', 'Houston Texans'],
    ['IND', 'Indianapolis', 'Colts', 'Indianapolos Colts'],
    ['JAX', 'Jacksonville', 'Jaguars', 'Jacksonville Jaguars'],
    ['KC', 'Kansas City', 'Chiefs', 'Kansas City Chiefs'],
    ['LA', 'Los Angeles', 'Rams', 'Los Angeles Rams'],
    ['LAC', 'Los Angeles', 'Chargers', 'Los Angeles Chargers'],
    ['MIA', 'Miami', 'Dolphins', 'Miami Dolphins'],
    ['MIN', 'Minnesota', 'Vikings', 'Minnesota Vikings'],
    ['NE', 'New England', 'Patriots', 'New England Patriots'],
    ['NO', 'New Orleans', 'Saints', 'New Orleans Saints'],
    ['NYG', 'New York', 'Giants', 'New York Giants'],
    ['NYJ', 'New York', 'Jets', 'New York Jets'],
    ['OAK', 'Oakland', 'Raiders', 'Oakland Raiders'],
    ['PHI', 'Philadelphia', 'Eagles', 'Philadelphia Eagles'],
    ['PIT', 'Pittsburgh', 'Steelers', 'Pittsburgh Steelers'],
    ['SEA', 'Seattle', 'Seahawks', 'Seattle Seahawks'],
    ['SD', 'San Diego', 'Chargers', 'San Diego Chargers'],
    ['SF', 'San Francisco', '49ers', 'San Francisco 49ers'],
    ['STL', 'St. Louis', 'Rams', 'St. Louis Rams'],
    ['TB', 'Tampa Bay', 'Buccaneers', 'Tampa Bay Buccaneers'],
    ['TEN', 'Tennessee', 'Titans', 'Tennessee Titans'],
    ['WAS', 'Washington', 'Redskins', 'Washington Redskins']
]
