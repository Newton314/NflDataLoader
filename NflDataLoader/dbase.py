import sqlite3 as lite
import pandas as pd

def retrieve_active_players(database="database/nfl.db"):
    ''' generator which returns all active players in the database'''
    con = lite.connect(database)
    with con:
        con.row_factory = lite.Row
        cur = con.cursor()
        command = '''
        SELECT * FROM Players WHERE Status='ACT';'''
        cur.execute(command)
        while True:
            row = cur.fetchone()
            if row is None:
                break
            yield row


def retrieve_dict(database="database/nfl.db"):
    con = lite.connect(database)
    with con:
        con.row_factory = lite.Row
        cur = con.cursor()
        cur.execute("Select Name, Position from Players where Team=:team", {"team":'CAR'})
        table = cur.fetchall()
        return table


def update_playerid(playername, team, playerid, database='database/nfl.db', tablename='Players'):
    parameters = (playerid, playername, team)
    con = lite.connect(database)
    with con:
        cur = con.cursor()
        command = '''
        UPDATE {} SET PlayerID=?
        WHERE Name=?
        AND TEAM=?;'''.format(tablename)
        cur.execute(command, parameters)


def create_database(dbname, tablename):
    database = "database/{}.db".format(dbname)
    con = lite.connect(database)
    with con:
        cur = con.cursor()
        cur.execute("drop table if exists {};".format(tablename))
        command = '''
            CREATE TABLE Players (
            ID INTEGER PRIMARY KEY,
            PlayerID TEXT,
            Trikotnummer INTEGER,
            Name TEXT,
            Position TEXT,
            Status TEXT,
            Groesse INTEGER,
            Gewicht INTEGER,
            Geburtsdatum DATE,
            Altr INTEGER,
            Exp INTEGER,
            College TEXT,
            Team TEXT );'''
        cur.execute(command)


def add_player_to_db(playerinfos: list):
    '''
    adds an player to the database
    needed infos:
    PlayerID, Trikotnummer, Name, Position, Status,
    Groesse, Gewicht, Geburtsdatum, Alter, Exp, college, Team
    '''
    con = lite.connect("database/nfl.db")
    with con:
        cur = con.cursor()
        cur.execute('''
                    insert into Players Values(
                    NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''',
                    playerinfos)


def create_activeplayer_dataframe():
    '''
    in future should update the status of the players in the database,
    returns dataframe with the players which are currently active
    '''
    listofplayers = []
    columns = ['ID', 'playerID', 'Trikotnummer', 'Name', 'Position', 'Status',
               'Groesse', 'Gewicht', 'Geburtsdatum', 'Alter', 'Exp', 'College', 'Team']
    for entry in retrieve_active_players():
        listofplayers.append(entry)
    playerframe = pd.DataFrame(listofplayers, columns=columns)
    del playerframe['ID']
    del playerframe['Status']
    return playerframe


if __name__ == "__main__":
#    create_database('nfl', 'Players')
#    update_playerid('Mike Adams', 'CAR', '00-0022247')
    dframe = create_activeplayer_dataframe()
    print(dframe.head())
