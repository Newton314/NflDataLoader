import re
from datetime import date

import pandas as pd
import requests as req
from bs4 import BeautifulSoup as BS


class PlayerDataLoader():

    def _convert_inch_to_cm(self, value):
        '''
        converts feet-inches to cm
        '''
        m = re.match(r"(?P<feet>\d)-(?P<inches>\d+)", value)
        feet = int(m.group('feet'))
        inches = int(m.group('inches'))
        return round(feet * 30.48 + inches * 2.54)
    
    def _convert_pounds_to_kg(self, value):
        '''
        converts pounds to rounded kg
        '''
        return round(float(value) * 0.4536)