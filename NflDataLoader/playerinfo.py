import re

import pandas as pd
import requests as req
from bs4 import BeautifulSoup as BS


def inch_to_cm(value: str) -> int:
    match = re.match(r"(?P<feet>\d)-(?P<inches>\d+)", value)
    feet = int(match.group('feet'))
    inches = int(match.group('inches'))
    return int(feet * 30.48 + inches * 2.54)


def pounds_to_kg(value: float) -> int:
    return int(float(value) * 0.4536)


def download_playerdata(playerid):
    URL = 'http://www.nfl.com/players/profile'
    content = {'id': playerid}
    response = req.get(URL, content)
    soup = BS(response.text, 'html.parser')
    try:
        playerinfo = soup.find(id='player-bio').find(class_='player-info')
        meta = get_meta_data(playerinfo)
        meta['playerID'] = [playerid]
    except KeyError:
        meta = None
    metaframe = pd.DataFrame(meta)
    return metaframe


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
    info1 = list(p[2].stripped_strings)
    college = list(p[4].stripped_strings)
    n = re.search(';', college[1])
    if n:
        college[1] = college[1][:n.start()]
    info = info1 + college
    for x in range(1, len(info), 2):
        info[x] = info[x][2:]
    exp = get_exp(list(p[5].stripped_strings))
    info = info + exp
    info = {info[x].lower(): [info[x+1]] for x in range(0, len(info), 2)}
    info['height'][0] = inch_to_cm(info['height'][0])
    info['age'][0] = int(info['age'][0])
    info['weight'][0] = pounds_to_kg(info['weight'][0])
    info['position'] = [position]
    info['number'] = [number]
    match = re.search(r'\s', full_name)
    info['name'] = [full_name[0] + '.' + full_name[match.end():]]
    info['full_name'] = [full_name]
    info['firstname'] = [full_name[:match.start()]]
    info['lastname'] = [full_name[match.end():]]
    return info


def get_exp(l):
    m = re.search(r'(\d+)', l[1])
    return ['experience', int(m.group(1))]


def load_playerdata(player_id):
    PATH = 'NflDataLoader/database/roster/players.csv'
    try:
        table = pd.read_csv(PATH, index_col=1)
        return table[table['playerID'].isin(player_id)]
    except:
        return pd.DataFrame()


def add_playerdata(player_ids):
    PATH = 'NflDataLoader/database/roster/players.csv'
    table = pd.DataFrame()
    try:
        table = pd.read_csv(PATH, index_col=1)
    finally:
        dataframe = pd.DataFrame()
        for i in player_ids:
            dataframe = dataframe.append(download_playerdata(i), ignore_index=True, sort=False)
        table = table.append(dataframe, ignore_index=True, sort=False)
        table.to_csv(PATH)
    return dataframe


def get_playerdata(playerIDs):
    pIDs = set(playerIDs)
    dataframe = load_playerdata(pIDs)
    if len(dataframe) == len(pIDs):
        return dataframe
    else:
        if dataframe.empty:
            return add_playerdata(pIDs)
        else:
            IDs = set(dataframe['playerID'])
            i = pIDs - IDs
            dataframe = dataframe.append(add_playerdata(i), ignore_index=True, sort=False)
            return dataframe



if __name__ == "__main__":
    load = {'id': '00-0028237'}
    df = get_playerdata([load['id']])
    print(df.head())
    # print(get_playerdata(['00-0028237', '00-0019596']))
