import re
from datetime import date

import pandas as pd
import requests as req
from bs4 import BeautifulSoup as BS


class PlayerDataLoader():
    pass


def convert_inch_to_cm(value):
    '''
    converts feet-inches to cm
    '''
    try:
        m = re.match(r"(?P<feet>\d)-(?P<inches>\d+)", value)
        feet = int(m.group('feet'))
        inches = int(m.group('inches'))
    except AttributeError:
        m = re.match(r"(?P<feet>\d)\'(?P<inches>\d+)\"", value)
        feet = int(m.group('feet'))
        inches = int(m.group('inches'))
    return round(feet * 30.48 + inches * 2.54)

def convert_pounds_to_kg(value):
    '''
    converts pounds to rounded kg
    '''
    return round(float(value) * 0.4536)