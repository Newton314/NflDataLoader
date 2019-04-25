from xml.sax import handler
from datetime import date

import requests

class NflHandler(handler.ContentHandler):
    def __init__(self, season, seasontype, week):
        self.games = []
        self.week = week
        self.season = season
        self.stype = seasontype
        self.d = date.today()

    def startElement(self, name, attrs):
        gdic = {}
        if name == 'g':
            gdic['eid'] = attrs['eid']
            gdic['home'] = attrs['h']
            gdic['away'] = attrs['v']
            gdic['gamekey'] = attrs['gsis']
            gdic['week'] = self.week
            gdic['season'] = self.season
            gdic['seasonType'] = self.stype
            day = int(attrs['eid'][6:8])
            month = int(attrs['eid'][4:6])
            if month < self.d.month or self.d.year > self.season:
                gdic['finished'] = True
            else:
                if day < self.d.day:
                    gdic['finished'] = True
                else:
                    gdic['finished'] = False
            self.games.append(gdic)

    def get_games(self):
        return self.games

