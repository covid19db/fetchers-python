import io
import requests
import pandas as pd
from pandas import DataFrame
from datetime import date, timedelta
from requests.exceptions import ConnectionError


def get_recent_apple_mobility_data() -> DataFrame:
    baseurl = 'https://covid19-static.cdn-apple.com/covid19-mobility-data/current/v3/en-us/applemobilitytrends-{}.csv'

    for delta in range(14):
        dt = date.today() - timedelta(delta)
        url = baseurl.format(dt.strftime("%Y-%m-%d"))
        try:
            request = requests.get(url, timeout=30)
            if request.status_code != 200:
                continue
            request.encoding = 'utf-8'
            return pd.read_csv(io.StringIO(request.text))
        except ConnectionError:
            continue
    return None
