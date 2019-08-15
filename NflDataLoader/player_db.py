# from datetime import date
from typing import Sequence
import sqlalchemy as db
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.exc import MultipleResultsFound

import pandas as pd

Base = declarative_base()
class Player(Base):
    __tablename__ = 'players'
    id = Column(Integer, primary_key=True)
    player_id = Column(String) # gsis id
    trikotnumber = Column(Integer)
    name = Column(String)
    position = Column(String)
    status = Column(String)
    height = Column(Integer)
    weight = Column(Integer)
    birthdate = Column(Date)
    age = Column(Integer)
    exp = Column(Integer)
    college = Column(String)
    team = Column(String)
    esb_id = Column(String)


    def __repr__(self):
        return f"Player: {self.id}, {self.name}, {self.status}"


    def asdict(self):
        return {
            'id': self.id,
            'player_id': self.player_id,
            'trikotnumber': self.trikotnumber,
            'name': self.name,
            'position': self.position,
            'status': self.status,
            'height': self.height,
            'weight': self.weight,
            'birthdate': self.birthdate,
            'age': self.age,
            'exp': self.exp,
            'college': self.college,
            'team': self.team,
            'esb_id': self.esb_id
        }


class Players():
    def __init__(self, path: str = 'NflDataLoader/database/nflplayers.db', echo: bool = False):
        db_path = "sqlite:///" + path
        self.engine = db.create_engine(db_path, echo=echo)
        self._create_playerdb()
        # create a sessionmaker and connect the Engine object to it
        self.SessionMaker = sessionmaker(bind=self.engine)
        self.SafeSession = scoped_session(self.SessionMaker)
        # create the session, which is the handler to the database
        # self.session = self.SafeSession()


    def _create_playerdb(self):
        Base.metadata.create_all(self.engine)


    def delete_playerdb(self):
        Base.metadata.drop_all(self.engine)


    def create_player(self, infos: dict):
        """creates a new entry for the database
        expects dict with subset of these keys:
        player_id, trikotnumber, name, position,
        status, height, weight, birthdate, age,
        exp, college, team, esb_id
        """
        new_player = Player(
            player_id=infos.get('player_id'),
            trikotnumber=infos.get('trikotnumber'),
            name=infos.get('name'),
            position=infos.get('position'),
            status=infos.get('status'),
            height=infos.get('height'),
            weight=infos.get('weight'),
            birthdate=infos.get('birthdate'),
            age=infos.get('age'),
            exp=infos.get('exp'),
            college=infos.get('college'),
            team=infos.get('team'),
            esb_id=infos.get('esb_id')
        )
        return new_player


    def add_player(self, newplayer: Player):
        """
        adds player to database if not already in it
        uses esb_id to distinguish between players
        """
        session = self.SafeSession()
        if newplayer.esb_id is not None:
            query = session.query(Player).filter_by(esb_id=newplayer.esb_id)
        elif newplayer.player_id is not None:
            query = session.query(Player).filter_by(player_id=newplayer.player_id)
        else:
            print(f'Missing ID for Player {newplayer.name}')
        try:
            match = query.one_or_none()
            if match is None:
                session.add(newplayer)
                session.commit()
            else:
                print(f"Player {newplayer.name} is already in the database.")
        except MultipleResultsFound:
            print(f"Multiple players with same esb_id {newplayer.esb_id} exist in database")
        finally:
            self.SafeSession.remove()


    def get_first_player(self):
        session = self.SafeSession()
        result = session.query(Player).first()
        self.SafeSession.remove()
        return result


    def get_player(self, gsis_id=None, esb_id=None):
        """returns the player with the given player_id (gsis) or esb_id"""
        session = self.SafeSession()
        if gsis_id is not None:
            result = session.query(Player).filter_by(player_id=gsis_id).one_or_none()
        elif esb_id is not None:
            result = session.query(Player).filter_by(esb_id=esb_id).one_or_none()
        else:
            # durch log ersetzen
            print(f"Gsis {gsis_id} or esb {esb_id} necessary.")
            self.SafeSession.remove()
            return None
        self.SafeSession.remove()
        return result


    def get_multiple_players(self, gsis_ids: Sequence = None, esb_ids: Sequence = None):
        """generator which returns all infos for players in gsis_ids or esb_ids"""
        session = self.SafeSession()
        if gsis_ids is not None:
            result = session.query(Player).filter(Player.player_id.in_(gsis_ids))
        elif esb_ids is not None:
            result = session.query(Player).filter(Player.esb_id.in_(esb_ids))
        else:
            self.SafeSession.remove()
            raise "You need to provide a list with ids."
        self.SafeSession.remove()
        for player in result:
            yield player.asdict()


    def get_active_players(self):
        """ returns a list of playerdictionaries from players whose status == 'ACT'"""
        session = self.SafeSession()
        players = [player.asdict() for player in session.query(Player).filter_by(status='ACT')]
        self.SafeSession.remove()
        return players


    def update_player(self, updated_player: Player, gsis_id: str = None, esb_id: str = None):
        """updates name, trikotnumber, status, height, weight, team,
        college, birthdate, age, exp of the player with the given player_id
        if there is no player with the given id adds it to the database
        """
        session = self.SafeSession()
        if esb_id is not None:
            player = self.get_player(esb_id=esb_id)
        elif gsis_id is not None:
            player = self.get_player(gsis_id=gsis_id)
        else:
            raise ValueError("You need to provide an player id")
        if player is None:
            self.add_player(updated_player)
        else:
            player.name = updated_player.name
            player.trikotnumber = updated_player.trikotnumber
            player.status = updated_player.status
            player.height = updated_player.height
            player.weight = updated_player.weight
            player.team = updated_player.team
            player.college = updated_player.college
            player.birthdate = updated_player.birthdate
            player.age = updated_player.age
            player.exp = updated_player.exp
            session.commit()
        self.SafeSession.remove()


    def update_player_status(self, player_id: str, status: str):
        """updates the status of the player with the given gsis_id"""
        session = self.SafeSession()
        player = session.query(Player).filter_by(player_id=player_id).one_or_none()
        if player is not None:
            player.status = status
        session.commit()
        self.SafeSession.remove()


if __name__ == "__main__":
    dbase = Players()
    p = dbase.get_active_players()
    df = pd.DataFrame(p)
    df.to_csv("active_players.csv")
    # active = dbase.get_active_players()
# add multiple objects to the database
# session.add_all([
#      User(name='wendy', fullname='Wendy Williams', nickname='windy'),
#      User(name='mary', fullname='Mary Contrary', nickname='mary'),
#      User(name='fred', fullname='Fred Flintstone', nickname='freddy')])
