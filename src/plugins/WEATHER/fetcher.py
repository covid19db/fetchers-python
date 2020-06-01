import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher
from functools import reduce
import datetime
import pickle
import json
import netCDF4
from requests import get
import os


from .utils import create_aggr_df



__all__ = ('METDailyWEatherFetcher',)

logger = logging.getLogger(__name__)


class METDailyWEatherFetcher(AbstractFetcher):
    LOAD_PLUGIN = True

    def fetch(self, DATERANGE):
        # load the variables dict
        with open("plugins/WEATHER/input/weather_indicators.json", "r") as read_file:
            weather_indicators = json.load(read_file)

        # load grid to GADM level 2 dict
        with open('plugins/WEATHER/input/adm_2_to_grid.pkl', 'rb') as handle:
            adm_2_to_grid = pickle.load(handle)

        # creates a dataframe for each variable and merge them
        dfs = [create_aggr_df(indicator, DATERANGE, weather_indicators,
                              adm_2_to_grid) for indicator in weather_indicators]
        df_final = reduce(lambda left,right: pd.merge(left,right,on=['day',
                                        'country', 'region', 'city']), dfs)
        return df_final

    def get_last_weather_date(self):
        sql = f"SELECT date FROM weather "
        date = pd.DataFrame(self.db.execute(sql), columns=["date"])

        return date.date.values[-1]

    def run(self):
        #Define date range
        print("defining date range")
        start = self.get_last_weather_date() + datetime.timedelta(days=1)
        stop = datetime.datetime.now() - datetime.timedelta(days=1)
        step = datetime.timedelta(days=1)
        DATERANGE = pd.date_range(start, stop, freq=step)

        print("fetching weather data")
        new_data = self.fetch(DATERANGE)
        new_data.to_pickle("plugins/WEATHER/out/weather_table_{}_{}.pkl".format(
                           start.strftime('%Y-%m-%d'),
                           stop.strftime('%Y-%m-%d')),
                           protocol=3)
