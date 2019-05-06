import re
from datetime import date

import pandas as pd
import requests as req

from player_db import Players
from roster import update_database, download_player_data

class PlayerDataLoader():
    def __init__(self, **kwargs):
        """
        optional parameters:
        'path': str, filepath for the players database
        'echo': bool
        """
        self.db_path = kwargs.get('path', 'NflDataLoader/database/nflplayers.db')
        self.db = Players(
            path=self.db_path,
            echo=kwargs.get('echo', False)
            )


    def update_database(self):
        self.db = update_database(self.db_path)


    def get_player_data(self, gsis_id) -> dict:
        data = self.db.get_player(player_id=gsis_id).asdict()
        if data is None:
            data = download_player_data(gsis_id)
            if data is not None:
                player = self.db.create_player(data)
                self.db.add_player(player)
            else:
                raise ValueError (f"For the player with id {gsis_id} no data available.")
        return data


    def get_multiple_player_data(self, gsis_ids=None, esb_ids=None):
        if gsis_ids is not None:
            return self.db.get_multiple_players(gsis_ids=gsis_ids)


def get_player_infos(player_ids: list) -> pd.DataFrame:
    ploader = PlayerDataLoader()
    infos = list()
    for p in ploader.get_multiple_player_data(gsis_ids=player_ids):
        infos.append(p)
    if len(player_ids) == len(infos):
        return pd.DataFrame(infos)
    else:
        player_ids = set(player_ids) - set([i['player_id'] for i in infos])
        for i in player_ids:
            infos.append(ploader.get_player_data(gsis_id=i))
        return pd.DataFrame(infos)


if __name__ == "__main__":
    ids = [
        "00-0031181",
        "00-0030465",
    ]
    infos = get_player_infos(ids)
    print(infos.head())
