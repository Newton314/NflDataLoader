import re
from datetime import date

import pandas as pd
import requests as req

from player_db import Players
from roster import update_database

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


    def get_player_data(self, player_id) -> dict:
        data = self.db.get_player(player_id).asdict()
        return pd.DataFrame(data)

