import pandas as pd

from .helperfunctions import convert_inch_to_cm, convert_pounds_to_kg
from .roster import download_player_ids


team_urls = {
    "ARI": "https://www.azcardinals.com/",
    "ATL": "https://www.atlantafalcons.com/",
    "BAL": "https://www.baltimoreravens.com/",
    'BUF': "https://www.buffalobills.com/",
    'CAR': "https://www.panthers.com/",
    'CHI': "https://www.chicagobears.com/",
    'CIN': "https://www.bengals.com/",
    'CLE': "https://www.clevelandbrowns.com/",
    'DAL': "https://www.dallascowboys.com/",
    'DEN': "https://www.denverbroncos.com/",
    'DET': "https://www.detroitlions.com/",
    'GB':  "https://www.packers.com/",
    'HOU': "https://www.houstontexans.com/",
    'IND': "https://www.colts.com/",
    'JAX': "https://www.jaguars.com/",
    'KC':  "https://www.chiefs.com/",
    'LAC': "https://www.chargers.com/",
    'LA':  "https://www.therams.com/",
    'MIA': "https://www.miamidolphins.com/",
    'MIN': "https://www.vikings.com/",
    'NE':  "https://www.patriots.com/",
    'NO':  "https://www.neworleanssaints.com/",
    'NYG': "https://www.giants.com/",
    'NYJ': "https://www.newyorkjets.com/",
    'OAK': "https://www.raiders.com/",
    'PHI': "https://www.philadelphiaeagles.com/",
    'PIT': "https://www.steelers.com/",
    'SEA': "https://www.49ers.com/",
    'SF':  "https://www.seahawks.com/",
    'TB':  "https://www.buccaneers.com/",
    'TEN': "https://www.titansonline.com/",
    'WAS': "https://www.redskins.com/",
}

def get_players(team: str):
    "returns active, reserve and other (e.g. practice squad)"
    url = team_urls.get(team, None)
    apendix = "team/players-roster"
    if url:
        url += apendix
        tables = pd.read_html(url)
        return tables
    return None

def download_injury_report(team: str):
    base_url = team_urls.get(team, None)
    apendix = "team/injury-report"
    if base_url:
        url = base_url + apendix
        tables = pd.read_html(url)
        return tables
    return None

def get_injured_players(team: str) -> list:
    injured, *_ = download_injury_report(team)
    # breakpoint()
    dnp = []
    if "Game Status" in injured.columns:
        dnp = injured[injured["Game Status"] == 'OUT']["Player"]
    elif "Wed" in injured.columns:
        dnp = injured[injured['Wed'] == 'DNP']['Player']
    elif "Thu" in injured.columns:
        dnp = injured[injured["Thu"] == "DNP"]["Player"]
    return list(dnp)

def get_active_players(team: str) -> pd.DataFrame:
    active, *_ = get_players(team)
    injured = get_injured_players(team)
    active['status'] = 1
    active.loc[active['Player'].isin(injured), 'status'] = 0
    active.HT = active.HT.apply(convert_inch_to_cm)
    active.WT = active.WT.apply(convert_pounds_to_kg)
    return active[active['status'] == 1].copy()

def build_active_players(team: str) -> pd.DataFrame:
    actives = get_active_players(team)
    del actives['status']
    actives = actives.rename(
        columns={'Player': 'name',
                 '#': 'trikotnumber',
                 'WT': 'weight',
                 'HT': 'height',
                 'Exp': 'exp',
                 'College': 'college',
                 'Pos': 'Position',})
    ids = download_player_ids(team)
    ids = pd.DataFrame(ids, index=('gsis_id', 'esb_id'))
    ids = ids.T
    ids = ids.reset_index()
    ids.rename(columns={'index': 'name'}, inplace=True)
    df = pd.merge(actives, ids, how='inner', on='name')
    df['team'] = team
    print(team)
    return df


def get_active_players_for_all_teams() -> pd.DataFrame:
    players = [build_active_players(team) for team in team_urls]
    df = pd.concat(players, sort=False, ignore_index=True)
    df.to_csv('active_players_2019-09-26.csv')
    return df


def get_depth_info(team: str):
    offense, defense = download_depth_chart(team)
    offense = process_depth_chart(offense)
    defense = process_depth_chart(defense)
    depth = pd.concat([offense, defense])
    depth.reset_index(drop=True, inplace=True)
    return depth


def download_depth_chart(team: str):
    appendix = "team/depth-chart"
    url = team_urls.get(team, None) + appendix
    frames = pd.read_html(url)
    offense, defense, *_ = frames
    return offense, defense


def process_depth_chart(dframe: pd.DataFrame):
    position = []
    player = []
    string = []
    for row in dframe.itertuples():
        position += [row.Position] * 3
        player += [row[-3], row[-2], row[-1]]
        string += [1, 2, 3]
    assert len(position) == len(player) == len(string)

    frame = pd.DataFrame()
    frame['player'] = player
    frame['position'] = position
    frame['string'] = string
    frame = frame.dropna(subset=["player"])
    frame = frame.sort_values(by=["string"])
    frame = frame.drop_duplicates(subset=["player"], keep='first')
    return frame
