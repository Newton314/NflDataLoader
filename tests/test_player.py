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
                            'player_id': "00-01", 'status': 'ACT'}
        player = self.players.create_player(self.test_player)
        self.players.add_player(player)


    def test_get_first_player(self):
        player = self.players.get_first_player()
        self.assertEqual(self.test_player['name'], player.name)
        self.assertEqual(self.test_player['birthdate'], player.birthdate)
        self.assertEqual(self.test_player['player_id'], player.player_id)


    def test_get_player(self):
        existing_player = self.players.get_player('00-01')
        self.assertEqual(existing_player.name, self.test_player['name'])
        non_existing = self.players.get_player('00-02')
        self.assertIsNone(non_existing)


    def test_update_player_status(self):
        new_status = 'INJ'
        self.players.update_player_status('00-01', new_status)
        updated_player = self.players.get_player('00-01')
        self.assertEqual(new_status, updated_player.status)


    def test_update_player(self):
        updated_player = self.players.create_player({
            'player_id': '00-01',
            'name': 'Updated Name',
            'status': 'Updated Status',
            'team': 'Updated Team'
        })
        new_player = self.players.create_player({
            'player_id': '00-02',
            'name': 'New Name',
            'team': 'New Team',
            'status': 'New Status'
        })
        self.players.update_player(updated_player.player_id, updated_player)
        player = self.players.get_player(updated_player.player_id)
        self.assertEqual(player.name, updated_player.name)
        self.assertEqual(player.team, updated_player.team)
        self.assertEqual(player.status, updated_player.status)
        self.players.update_player(new_player.player_id, new_player)
        player = self.players.get_player(new_player.player_id)
        self.assertEqual(player.player_id, new_player.player_id)
        self.assertEqual(player.name, new_player.name)
        self.assertEqual(player.team, new_player.team)
        self.assertEqual(player.status, new_player.status)


    def tearDown(self):
        p = Path("tests/fixtures/test.db")
        if p.exists():
            p.unlink()
