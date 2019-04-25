from datetime import date

import sqlalchemy as db
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


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


    def add_player(self, player):
        self.session.add(player)
        self.session.commit()


    def get_first_player(self):
        p = self.session.query(Player).first()
        return p

dbase = Players()
new = dbase.create_player({'name': "Test Player",
                           'position': 'WR', 'trikotnumber': 1, 'age': 20,
                           'birthdate': date.today()})
dbase.add_player(new)

q = dbase.get_first_player()
print(q.id, q.name, q.position, q.birthdate)


'''
add multiple objects to the database
session.add_all([
     User(name='wendy', fullname='Wendy Williams', nickname='windy'),
     User(name='mary', fullname='Mary Contrary', nickname='mary'),
     User(name='fred', fullname='Fred Flintstone', nickname='freddy')])
'''
