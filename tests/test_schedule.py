import unittest

from datetime import date

from NflDataLoader.scheduleloader import ScheduleLoader, create_date_from_eid


class TestScheduleLoader(unittest.TestCase):
    def test_basic_schedule(self):
        s = ScheduleLoader("test", "test_sched", update=False)
        test_schedule = s.schedule
        test_game = test_schedule[0]
        self.assertEqual(test_game['entry1'], 1)
        self.assertEqual(test_game['entry2'], 2)

    def test_get_previous_season(self):
        '''
        tests for the correct previous season (< march).
        '''
        loader = ScheduleLoader("test", "test_sched", update=False)
        d = date(2019, 2, 28)
        season = loader.get_season(dte=d)
        self.assertEqual(season, 2018)


    def test_get_current_season(self):
        '''
        tests for the correct current season (>= march).
        '''
        loader = ScheduleLoader("test", "test_sched", update=False)
        d = date(2019, 3, 1)
        season = loader.get_season(dte=d)
        self.assertEqual(season, 2019)


    def test_create_date_from_eid(self):
        year = 1992
        month = 8
        day = 15
        d = date(year, month, day)
        eid = f"{year}0{month}{day}"
        self.assertEqual(create_date_from_eid(eid), d)


if __name__ == "__main__":
    unittest.main()
