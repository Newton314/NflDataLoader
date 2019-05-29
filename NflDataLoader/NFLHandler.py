from xml.sax import handler
from datetime import date


class NflHandler(handler.ContentHandler):
    def __init__(self, season, seasontype, week):
        super().__init__()
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
            gamedate = date(self.season, month, day)
            if gamedate < self.d:
                gdic['finished'] = True
            else:
                gdic['finished'] = False
            self.games.append(gdic)

    def get_games(self):
        return self.games
