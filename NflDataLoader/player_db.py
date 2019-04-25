from datetime import date

import sqlalchemy as db
from sqlalchemy import Column, Integer, String, Date, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound


Base = declarative_base()
class Player(Base):
    __tablename__ = 'players'
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer)
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

    def __repr__(self):
        return f"Player: {self.id}, {self.name}, {self.status}"


class Players():
    def __init__(self, path='NflDataLoader/database/nflplayers.db', echo=True):
        db_path = "sqlite:///" + path
        self.engine = db.create_engine(db_path, echo=echo)
        self.create_playerdb()
        # create a sessionmaker and connect the Engine object to it
        self.SessionMaker = sessionmaker(bind=self.engine)
        # create the session, which is the handler to the database
        self.session = self.SessionMaker()


    def create_playerdb(self):
        Base.metadata.create_all(self.engine)


    def delete_playerdb(self):
        Base.metadata.drop_all(self.engine)


    def create_player(self, infos: dict):
        """creates a new entry for the database
        expects dict with subset of these keys:
        player_id, trikotnumber, name, position,
        status, height, weight, birthdate, age,
        exp, college, team
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
        p = self.session.query(Player).first()
        return p


    def get_active_players(self):
        active_players = self.session.query(Player).filter_by(status='ACT')
        print(active_players.all())
        return "active"


if __name__ == "__main__":    
    dbase = Players(echo=False)
    new = dbase.create_player({'name': "Test Player", 'player_id': 1,
                               'position': 'WR', 'trikotnumber': 1, 'age': 20,
                               'birthdate': date.today(), 'status':'ACT'})
    dbase.add_player(new)
    new2 = dbase.create_player({'name': "Inactive", "status":'', 'player_id': 2})
    dbase.add_player(new2)
    new3 = dbase.create_player({'name': "Active Player", "player_id": 3, "status": 'ACT'})
    dbase.add_player(new3)

    q = dbase.get_first_player()
    print(q)
    active = dbase.get_active_players()


'''
add multiple objects to the database
session.add_all([
     User(name='wendy', fullname='Wendy Williams', nickname='windy'),
     User(name='mary', fullname='Mary Contrary', nickname='mary'),
     User(name='fred', fullname='Fred Flintstone', nickname='freddy')])
'''
