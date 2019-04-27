import unittest
from datetime import date
from pathlib import Path

from NflDataLoader.playerdataloader import convert_pounds_to_kg, convert_inch_to_cm
from NflDataLoader.player_db import Players

class TestPlayerLoader(unittest.TestCase):
    def test_convert_inch_to_cm_bindestreich(self):
        feet = 6
        inches = 2
        value = f"{feet}-{inches}"
        cm = convert_inch_to_cm(value)
        self.assertEqual(cm, 188)


    def test_convert_inch_to_cm_anführungszeichen(self):
        feet = 6
        inches = 2
        value = f"{feet}\'{inches}\""
        cm = convert_inch_to_cm(value)
        self.assertEqual(cm, 188)


    def test_convert_pounds_to_kg(self):
        pounds = 10
        kg = convert_pounds_to_kg(pounds)
        self.assertEqual(kg, 5)

class TestPlayerDB(unittest.TestCase):
    def setUp(self):
        today = date.today()
        self.players = Players(path='tests/fixtures/test.db')
        self.test_player = {'name': 'Test Player', 'position': 'test_position',
                            'trikotnumber': 3, 'birthdate': today,
                            'player_id': str(314), 'status': 'ACT'}
        player = self.players.create_player(self.test_player)
        self.players.add_player(player)


    def test_get_first_player(self):
        player = self.players.get_first_player()
        self.assertEqual(self.test_player['name'], player.name)
        self.assertEqual(self.test_player['birthdate'], player.birthdate)
        self.assertEqual(self.test_player['player_id'], player.player_id)


    def tearDown(self):
        p = Path("tests/fixtures/test.db")
        if p.exists():
            p.unlink()
