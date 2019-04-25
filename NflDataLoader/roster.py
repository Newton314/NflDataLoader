import re
from datetime import date

import requests
from bs4 import BeautifulSoup as BS
from tqdm import tqdm

from playerinfo import pounds_to_kg, inch_to_cm
import dbase

# profile_url = 'http://www.nfl.com/players/profile'
# profile_load = {'id': '00-0028735'}

TEAMS = [
    'ARI',
    'ATL',
    'BAL',
    'BUF',
    'CAR',
    'CHI',
    'CIN',
    'CLE',
    'DAL',
    'DEN',
    'DET',
    'GB',
    'HOU',
    'IND',
    'JAX',
    'KC',
    'LA',
    'LAC',
    'MIA',
    'MIN',
    'NE',
    'NO',
    'NYG',
    'NYJ',
    'OAK',
    'PHI',
    'PIT',
    'SEA',
    'SF',
    'TB',
    'TEN',
    'WAS',
]


def download_roster(team):
    roster_url = 'http://www.nfl.com/teams/roster'
    roster_load = {'team': team}

    response = requests.get(roster_url, roster_load)

    soup = BS(response.text, 'html.parser')

    tbodys = soup.find(id='result').find_all('tbody')

    roster = []
    for row in tbodys[len(tbodys) - 1].find_all('tr'):
        try:
            tds, data = [], []
            for td in row.find_all('td'):
                tds.append(td)
                data.append(td.get_text().strip())
            name = tds[1].a.get_text().strip()
            d = {
                'number': data[0],
                'name': name,
                'position': data[2],
                'status': data[3],
                'height': data[4],
                'weight': data[5],
                'birthdate': data[6],
                'experience': data[7],
                'college': data[8],
                'team': team
            }
            roster.append(d)
        except Exception:
            print('Error')
    return roster


def convert_roster(roster):
    today = date.today()
    for player in roster:
        player['height'] = inch_to_cm(player['height'])
        player['weight'] = pounds_to_kg(int(player['weight']))
        name = player['name']
        m = re.match(r"(?P<lastname>[\S\s]+), (?P<firstname>\S+)", name)
        player['name'] = m.group('firstname') + " " + m.group('lastname')
        split = re.split(r'/', player['birthdate'])
        month, day, year = int(split[0]), int(split[1]), int(split[2])
        bday = date(year, month, day)
        player['birthdate'] = bday.isoformat()
        age = today - bday
        age = age.days
        age = int(age / 365)
        player['age'] = age
        player['playerID'] = str(1)
        player['experience'] = int(player['experience'])
        try:
            player['number'] = int(player['number'])
        except ValueError:
            player['number'] = 0
    return roster


def prepare_db_entry(playerdict):
    dic = playerdict
    player = []
    player.append(dic['playerID'])
    player.append(dic['number'])
    player.append(dic['name'])
    player.append(dic['position'])
    player.append(dic['status'])
    player.append(dic['height'])
    player.append(dic['weight'])
    player.append(dic['birthdate'])
    player.append(dic['age'])
    player.append(dic['experience'])
    player.append(dic['college'])
    player.append(dic['team'])
    return player


def add_team_to_database(roster):
    roster = convert_roster(roster)
    for player in roster:
        entry = prepare_db_entry(player)
        dbase.add_player_to_db(entry)


def create_new_database():
    dbase.create_database("nfl", "Players")
    for team in tqdm(TEAMS, desc="Teams"):
        team_roster = download_roster(team)
        add_team_to_database(team_roster)



if __name__ == '__main__':
    create_new_database()
