# roster.py
import re
from datetime import date
from typing import Tuple, List
from pathlib import Path

import requests
from requests.exceptions import ConnectTimeout
from bs4 import BeautifulSoup as BS
from tqdm import tqdm

from player_db import Players
from helperfunctions import convert_inch_to_cm, convert_pounds_to_kg


# profile_url = 'http://www.nfl.com/players/profile'
# profile_load = {'id': '00-0028735'}

Player_IDs = Tuple[str]
Roster = List[dict]

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

def get_meta_data(playerinfo):
    full_name = playerinfo.find(class_='player-name').get_text().strip()
    try:
        number = playerinfo.find(class_='player-number').get_text()
        match = re.search(r'#(?P<number>\d+)\s(?P<position>[A-Z]+)', number)
        number = match.group('number')
        position = match.group('position')
    except AttributeError:
        number = 0
        position = None
    p = playerinfo.find_all('p')
    for entry in p:
        if 'Height' in str(entry):
            info1 = list(entry.stripped_strings)
            continue
        if 'College' in str(entry):
            college = list(entry.stripped_strings)
            break
    # info1 = list(p[2].stripped_strings)
    # college = list(p[4].stripped_strings)
    n = re.search(';', college[1])
    if n:
        college[1] = college[1][:n.start()]
    info = info1 + college
    for x in range(1, len(info), 2):
        info[x] = info[x][2:]
    info = {info[x].lower(): info[x+1] for x in range(0, len(info), 2)}
    try:
        info['exp'] = get_exp(list(p[5].stripped_strings))
    except IndexError:
        exp = ''
        for entry in p:
            if 'Experience' in str(entry):
                exp = entry
                break
        info['exp'] = get_exp(list(exp.stripped_strings))
    info['height'] = convert_inch_to_cm(info['height'])
    info['age'] = int(info['age'])
    info['weight'] = convert_pounds_to_kg(info['weight'])
    info['position'] = position
    info['trikotnumber'] = int(number)
    info['name'] = full_name
    return info


def get_exp(line: str) -> int:
    m = re.search(r'(\d+)', line[1])
    return int(m.group(1))


def download_player_data(gsis_id: str) -> dict:
    """Downloads the data for the player with the given gsis_id"""
    URL = 'http://www.nfl.com/players/profile'
    content = {'id': gsis_id}
    response = requests.get(URL, content, timeout=5)
    soup = BS(response.text, 'html.parser')
    try:
        playerinfo = soup.find(id='player-bio').find(class_='player-info')
        meta = get_meta_data(playerinfo)
        meta['player_id'] = gsis_id
        index = response.text.find('ESB ID')
        meta['esb_id'] = response.text[index+8:index+17]
    except KeyError:
        meta = None
    return meta


def download_roster(team: str) -> Roster:
    roster_url = 'http://www.nfl.com/teams/roster'
    roster_load = {'team': team}
    try:
        response = requests.get(roster_url, roster_load, timeout=5)
    except ConnectTimeout:
        print(response.url)
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
            player_url = "http://www.nfl.com" + tds[1].a.get('href')
            gsis_id, esb_id = get_player_ids(player_url)
            d = {
                'player_id': gsis_id,
                'trikotnumber': data[0],
                'name': name,
                'position': data[2],
                'status': data[3],
                'height': data[4],
                'weight': data[5],
                'birthdate': data[6],
                'exp': data[7],
                'college': data[8],
                'team': team,
                'esb_id': esb_id
            }
            roster.append(d)
        except:
            print("Error during roster download.")
    return roster


def get_player_ids(url: str) -> Player_IDs:
    try:
        response = requests.get(url, timeout=10)
    except ConnectTimeout:
        print(f"Timout while connecting to {url}")
        return ("gsis0000", "esb0000")
    if response.status_code == 200:
        text = response.text
        index = text.find('GSIS')
        gsis_id = text[index+9:index+19]
        if not gsis_id[-4:].isdigit():
            gsis_id = "0000"
        index = text.find('ESB ID')
        esb_id = text[index+8:index+17]
    else:
        gsis_id = "gsis0000"
        esb_id = "esb0000"
    return (gsis_id, esb_id)


def convert_roster(roster: Roster):
    today = date.today()
    for player in roster:
        player['height'] = convert_inch_to_cm(player['height'])
        player['weight'] = convert_pounds_to_kg(int(player['weight']))
        name = player['name']
        match = re.match(r"(?P<lastname>[\S\s]+), (?P<firstname>\S+)", name)
        player['name'] = match.group('firstname') + " " + match.group('lastname')
        try:
            split = re.split(r'/', player['birthdate'])
            month, day, year = int(split[0]), int(split[1]), int(split[2])
            bday = date(year, month, day)
        except ValueError:
            bday = today
        player['birthdate'] = bday
        age = today - bday
        age = age.days
        age = int(age / 365)
        player['age'] = age
        player['exp'] = int(player['exp'])
        try:
            player['trikotnumber'] = int(player['trikotnumber'])
        except ValueError:
            player['trikotnumber'] = 0
    return roster


def create_db_entries(db: Players, roster: Roster) -> list:
    roster = convert_roster(roster)
    players = list()
    for player in roster:
        players.append(db.create_player(player))
    return players


def create_new_database(PATH: Path = Path('NflDataLoader/database/nflplayers.db'), **kwargs):
    db = Players(path=str(PATH), echo=kwargs.get('echo', False))
    for team in tqdm(TEAMS, desc="Downloading Roster..."):
        team_roster = download_roster(team)
        roster = create_db_entries(db, team_roster)
        for player in roster:
            db.add_player(player)
    return db


def update_database(PATH: Path = Path('NflDataLoader/database/nflplayers.db')):
    if PATH.exists():
        db = Players(path=str(PATH))
        for team in tqdm(TEAMS, desc="Updating Database..."):
            team_roster = download_roster(team)
            team_roster = create_db_entries(db, team_roster)
            for player in team_roster:
                db.update_player(player, esb_id=player.esb_id)
    else:
        db = create_new_database(PATH=PATH)
    return db


if __name__ == '__main__':
    # dbase = update_database()

    # print(dbase.get_first_player().asdict())
    # url = "http://www.nfl.com/player/jordanbrown/2562396/profile"
    # i, d = get_player_ids(url)
    # print(i,d)
    # r = download_roster('ATL')
    # for p in r:
    #     print(p)
    i = "00-0029248" # Luke Kuechly
    print(download_player_data(i))
