import requests as req
import pandas as pd
import re
import json
from os import listdir

# gets data from airyards.com

def get_index(table, name, week):
    t2 = table[table['name'].isin([name])]
    t2 = t2[t2['week'].isin([week])]
    if not t2.empty:
        return t2.index[0]
    else:
        return False


def get_airjson(season):
    PATH = "database/jsonarchive/airyards/"
    jsonfile = "{}.json".format(season)
    filepath = PATH + jsonfile
    if jsonfile in listdir(PATH):
        with open(filepath, "r") as infile:
            return json.load(infile)
    url = 'http://airyards.com/{}/weeks'.format(season)
    res = req.get(url)
    if res.status_code == req.codes.ok:
        filepath = PATH + jsonfile
        with open(filepath, 'w') as outfile:
            json.dump(res.json(), outfile)
        return res.json()
    else:
        print('No connection to airyards.com')
        return False

def get_airtable(season, team, week):
    js = get_airjson(season)
    table = pd.DataFrame(js)
    teamtable = table[table['team'].isin([team])]
    teamtable = teamtable[teamtable['week'].isin([week])]
    todelete = ['team', 'index', 'rec_yards', 'week', 'rec', 'tm_att', 'position']
    for item in todelete:
        if item in teamtable.columns:
            del teamtable[item]
    return convert_names(teamtable)

def convert_names(table):
    names = list(table['full_name'])
    for name in names:
        if name:
            m = re.search(' ', name)
            names[names.index(name)] = name[0] + '.' + name[m.end():]
        else:
            names[names.index(name)] = ''
#            i = table[table['full_name'].isin([name])].index[0]
#            pID = table.loc[i, 'player_id']
#            full_name = get_playername(pID)
#            if full_name:
#                m = re.search(' ', full_name)
#                names[names.index(name)] = full_name[0] + '.' + full_name[m.end():]
#            else:
#                table = table.drop([i], axis=0)
    table['full_name'] = names
    cols = list(table.columns)
    cols[cols.index('full_name')] = 'name'
    table.columns = cols
    if 'player_id' in table.columns:
        del table['player_id']
    return table
    
if __name__ == '__main__':
    print(pd.DataFrame(get_airjson(2016)).head())

