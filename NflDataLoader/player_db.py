# from datetime import date
import sqlalchemy as db
from sqlalchemy import Column, Integer, String, Date, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
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
        # create the session, which is the handler to the database
        self.session = self.SessionMaker()


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


    def add_player(self, newplayer):
        query = self.session.query(Player)\
            .filter(or_(Player.name == newplayer.name, Player.player_id == newplayer.player_id))
        try:
            match = query.one_or_none()
            if not match:
                self.session.add(newplayer)
                self.session.commit()
            # else update player ?
        except MultipleResultsFound:
            print("Multiple players exist in database")


    def get_first_player(self):
        return self.session.query(Player).first()


    def get_player(self, player_id):
        """returns the player with the given player_id"""
        return self.session.query(Player).filter_by(player_id=player_id).one_or_none()


    def get_active_players(self):
        """ returns a list of playerdictionaries from players whose status == 'ACT'"""
        return [player.asdict() for player in self.session.query(Player).filter_by(status='ACT')]


    def update_player(self, player_id: str, updated_player: Player):
        """updates name, trikotnumber, status, height, weight, team,
        college, birthdate, age, exp of the player with the given player_id
        if there is no player with the given player_id adds it to the database
        """
        player = self.session.query(Player).filter_by(player_id=player_id).one_or_none()
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
            self.session.commit()


    def update_player_status(self, player_id: str, status: str):
        """updates the status of the player with the given player_id"""
        player = self.session.query(Player).filter_by(player_id=player_id).one_or_none()
        if player is not None:
            player.status = status


if __name__ == "__main__":
    dbase = Players(echo=False, path='test.db')
    p = dbase.create_player({
        'player_id': '00-01',
        'name': 'Test Player',
        'status': 'INJ'
    })
    up = dbase.create_player({
        'player_id': '00-01',
        'name': 'Test Player',
        'status': 'ACT'
    })
    dbase.add_player(p)
    print(dbase.get_first_player().asdict())
    dbase.update_player(up.player_id, up)
    print(dbase.get_first_player().asdict())
    f = dbase.get_first_player()
    # active = dbase.get_active_players()
# add multiple objects to the database
# session.add_all([
#      User(name='wendy', fullname='Wendy Williams', nickname='windy'),
#      User(name='mary', fullname='Mary Contrary', nickname='mary'),
#      User(name='fred', fullname='Fred Flintstone', nickname='freddy')])
