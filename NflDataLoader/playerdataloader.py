# playerdataloader.py
from typing import Sequence
import pandas as pd

from .player_db import Players
from .roster import update_database, download_player_data

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
        """updates the connected database"""
        self.db = update_database(self.db_path)


    def get_player_data(self, gsis_id) -> dict:
        """return infos for the player with the given gsis id as dict"""
        data = self.db.get_player(player_id=gsis_id)
        if data is not None:
            return data.asdict()
        data = download_player_data(gsis_id)
        if data is not None:
            player = self.db.create_player(data)
            self.db.add_player(player)
        else:
            raise ValueError(f"For the player with id {gsis_id} no data available.")
        return data


    def get_multiple_player_data(self, gsis_ids: Sequence = None,
                                 esb_ids: Sequence = None) -> pd.DataFrame:
        if gsis_ids is not None:
            for player in self.db.get_multiple_players(gsis_ids=gsis_ids):
                yield player
        if esb_ids is not None:
            raise NotImplementedError
        return None


    def get_active_players(self):
        return self.db.get_active_players()


def get_player_infos(gsis_ids: Sequence) -> pd.DataFrame:
    """return infos for players with the given gsis_ids as panda DataFrame"""
    ploader = PlayerDataLoader()
    infos = []
    for p in ploader.get_multiple_player_data(gsis_ids=gsis_ids):
        infos.append(p)
    if len(gsis_ids) == len(infos):
        return pd.DataFrame(infos)
    gsis_ids = set(gsis_ids) - {i['player_id'] for i in infos}
    for i in gsis_ids:
        infos.append(ploader.get_player_data(gsis_id=i))
    return pd.DataFrame(infos)


if __name__ == "__main__":
    ids = [
        "00-0029248",
        "00-0030465",
    ]
    # info = get_player_infos(ids)
    info = download_player_data(ids[0])
    print(info)
