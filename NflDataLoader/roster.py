# roster.py
import re
from datetime import date
from typing import Tuple, List
from pathlib import Path
from dateutil.parser import parse

import requests
from requests.exceptions import ConnectTimeout
from bs4 import BeautifulSoup as BS
from tqdm import tqdm

from .player_db import Players
from .helperfunctions import convert_inch_to_cm, convert_pounds_to_kg


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

def get_dummy_meta_data(gsis_id):
    meta = {}
    meta['height'] = 0
    meta['weight'] = 0
    meta['age'] = 0
    meta['birthdate'] = date(1950, 1, 1)
    meta['college'] = 'None'
    meta['exp'] = 0
    meta['position'] = 'NA'
    meta['trikotnumber'] = 0
    meta['name'] = 'NA'
    meta['player_id'] = gsis_id
    meta['esb_id'] = 'NA'
    return meta


def get_meta_data(playerinfo):
    full_name = playerinfo.find(class_='player-name').get_text().strip()
    try:
        number = playerinfo.find(class_='player-number').get_text()
        match = re.search(r'#(?P<number>\d+)\s(?P<position>[A-Z]+)', number)
        number = match.group('number')
        position = match.group('position')
    except AttributeError:
        number = 0
        position = 'RET'
    p = playerinfo.find_all('p')
    info = {}
    # breakpoint()
    for entry in p:
        content = list(entry.stripped_strings)
        if 'Height' in content:
            height = content[content.index('Height') + 1]
            height = convert_inch_to_cm(height[2:])
            info['height'] = height
        if "Weight" in content:
            weight = content[content.index("Weight") + 1]
            m = re.search(r"\d+", weight)
            if m is not None:
                weight = weight[m.start():m.end()]
                weight = convert_pounds_to_kg(weight)
                info["weight"] = weight
        if "Age" in content:
            age = content[content.index("Age") + 1]
            info["age"] = int(age[2:])
        if 'Born' in content:
            birthday = content[content.index("Born") + 1]
            birthday = find_date(birthday)
            info["birthdate"] = birthday
        if 'College' in content:
            info['college'] = content[1][2:]
        if "Experience" in content:
            exp = get_exp(content)
            info['exp'] = exp
    # breakpoint()
    _ = info.setdefault("age", get_age_from_bday(info.get("birthdate", date.today())))
    info['position'] = position
    info['trikotnumber'] = int(number)
    info['name'] = full_name
    return info


def find_date(dt):
    m = re.search(r"(\d+)/(\d+)/(\d+)", dt)
    d = parse(dt[m.start():m.end()])
    return d.date()


def get_exp(line: str) -> int:
    m = re.search(r'(\d+)', line[1])
    try:
        return int(m.group(1))
    except AttributeError:
        return 0


def get_age_from_bday(bday: date) -> int:
    today = date.today()
    td = today - bday
    age = td.days // 365
    return age


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
    except AttributeError:
        # durch log ersetzen
        print(f"Spieler mit {gsis_id} nicht gefunden")
        meta = get_dummy_meta_data(gsis_id)
    return meta


def find_player_infos(html_text, team: str):
    soup = BS(html_text, 'html.parser')
    tbodys = soup.find(id='result').find_all('tbody')
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
                'height': data[4] if data[4] else "0'0\"",
                'weight': data[5] if data[5] else "0",
                'birthdate': data[6],
                'exp': data[7],
                'college': data[8],
                'team': team,
                'esb_id': esb_id
            }
            yield d
        except NameError as e:
            print(e)


def download_roster(team: str):
    roster_url = 'http://www.nfl.com/teams/roster'
    roster_load = {'team': team}
    try:
        response = requests.get(roster_url, roster_load, timeout=5)
    except ConnectTimeout:
        print(response.url)
    roster = []
    for player in find_player_infos(response.text, team):
        roster.append(player)
    return roster

def get_player_links(team: str):
    url = "http://nfl.com/teams/roster"
    load = {'team': team}
    try:
        response = requests.get(url, load, timeout=10)
        player_links = extract_links(response.text)
        return player_links
    except ConnectTimeout:
        raise ConnectTimeout(f"Connection to {response.url} timed out!")

def extract_links(html_text):
    soup = BS(html_text, 'lxml')
    tables = soup.find(id='result').find_all('tbody')
    refs = tables[-1].find_all('a')
    prefix = "http://www.nfl.com"
    urls = [prefix + ref.get('href') for ref in refs]
    names = [convert_name(ref.text) for ref in refs]
    player_urls = dict(zip(names, urls))
    return player_urls

def convert_name(old_name: str) -> str:
    match = re.match(r"(?P<lastname>[\S\s]+), (?P<firstname>\S+)", old_name)
    return match.group('firstname') + " " + match.group('lastname')


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


def download_player_ids(team: str):
    links = get_player_links(team)
    # multithreading?!
    ids = {name: get_player_ids(url) for name, url in links.items()}
    return ids


def convert_roster(roster: Roster) -> Roster:
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
        age = get_age_from_bday(bday)
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
