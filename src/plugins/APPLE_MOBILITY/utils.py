import re
import io
import requests
import pandas as pd
from pandas import DataFrame
from bs4 import BeautifulSoup
from datetime import date, timedelta
from requests_html import HTMLSession
from requests.exceptions import ConnectionError


def get_recent_apple_mobility_data() -> DataFrame:
    baseurl = 'https://covid19-static.cdn-apple.com/covid19-mobility-data/2008HotfixDev37/v3/en-us/applemobilitytrends-{}.csv'

    for delta in range(14):
        dt = date.today() - timedelta(delta)
        url = baseurl.format(dt.strftime("%Y-%m-%d"))
        try:
            request = requests.get(url)
            if request.status_code != 200:
                continue
            return pd.read_csv(io.StringIO(request.text))
        except ConnectionError:
            continue
    return None


def get_mobility_report_urls():
    """
    DESCRIPTION:
    This function gets Apple Mobility Report PDF links from:
    https://covid19-static.cdn-apple.com

    :return: [list] list of Apple Mobility Report urls
    """
    session = HTMLSession()
    resp = session.get('https://www.apple.com/covid19/mobility')
    resp.html.render()
    soup = BeautifulSoup(resp.html.html, 'lxml')
    return [link.get('href') for link in
            soup.findAll('a', attrs={'href': re.compile("^https://covid19-static.cdn-apple.comm")})]


def get_country_codes():
    """
    DESCRIPTION:
    This function returns ISO country codes

    :return: [pandas DataFrame] ISO country codes.
    """
    return pd.read_csv('http://geohack.net/gis/wikipedia-iso-country-codes.csv')


def get_country_info(country_codes: DataFrame, country_a2_code: str = None, country_name: str = None):
    try:
        if country_a2_code and pd.notna(country_a2_code):
            country_info = country_codes[country_codes['Alpha-2 code'] == country_a2_code].to_dict('records')[0]
        else:
            country_info = \
                country_codes[country_codes['English short name lower case'] == country_name].to_dict('records')[0]

        countrycode = country_info["Alpha-3 code"]
        country = country_info["English short name lower case"]
    except Exception as ex:
        return None, None

    return country, countrycode
