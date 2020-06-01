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


class METDailyWeatherFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'MET'

    def fetch(self, DATERANGE):
        # load the variables dict
        with open("plugins/WEATHER/input/weather_indicators.json", "r") as read_file:
            weather_indicators = json.load(read_file)

        # load grid to GADM level 2 dict
        with open('plugins/WEATHER/input/adm_2_to_grid.pkl', 'rb') as handle:
            adm_2_to_grid = pickle.load(handle)

        # creates a dataframe for each variable and merge them
        dfs = [create_aggr_df(indicator, DATERANGE, weather_indicators,
                              adm_2_to_grid, logger) for indicator in weather_indicators]
        df_final = reduce(lambda left,right: pd.merge(left,right,on=['day',
                                        'country', 'region', 'city']), dfs)
        return df_final

    def get_last_weather_date(self):
        sql = f"SELECT date FROM weather "
        date = pd.DataFrame(self.db.execute(sql), columns=["date"])

        return date.date.values[-1]

    def run(self):
        #Define date range
        logger.debug("defining date range")
        start = self.get_last_weather_date() + datetime.timedelta(days=1)
        stop = datetime.datetime.now() - datetime.timedelta(days=1)
        step = datetime.timedelta(days=1)
        DATERANGE = pd.date_range(start, stop, freq=step)

        logger.debug("fetching weather data")
        new_data = self.fetch(DATERANGE)

        # save to pickle file
        # new_data.to_pickle("plugins/WEATHER/weather_table_{}_{}.pkl".format(
        #                            start.strftime('%Y-%m-%d'),
        #                            stop.strftime('%Y-%m-%d')),
        #                            protocol=3)

        for index, row in new_data.iterrows():
            upsert_obj = {
                'date': row['day'],
                'countrycode': row['country'],
                'gid': row['city'],
                'precip_max_avg': row['precip_max_avg'],
                'precip_max_std': row['precip_max_std'],
                'precip_mean_avg': row['precip_mean_avg'],
                'precip_mean_std': row['precip_mean_std'],
                'specific_humidity_max_avg': row['specific_humidity_max_avg'],
                'specific_humidity_max_std': row['specific_humidity_max_std'],
                'specific_humidity_mean_avg': row['specific_humidity_mean_avg'],
                'specific_humidity_mean_std': row['specific_humidity_mean_std'],
                'specific_humidity_min_avg': row['specific_humidity_min_avg'],
                'specific_humidity_min_std': row['specific_humidity_min_std'],
                'short_wave_radiation_max_avg': row['short_wave_radiation_max_avg'],
                'short_wave_radiation_max_std': row['short_wave_radiation_max_std'],
                'short_wave_radiation_mean_avg': row['short_wave_radiation_mean_avg'],
                'short_wave_radiation_mean_std': row['short_wave_radiation_mean_std'],
                'air_temperature_max_avg': row['air_temperature_max_avg'],
                'air_temperature_max_std': row['air_temperature_max_std'],
                'air_temperature_mean_avg': row['air_temperature_mean_avg'],
                'air_temperature_mean_std': row['air_temperature_mean_std'],
                'air_temperature_min_avg': row['air_temperature_min_avg'],
                'air_temperature_min_std': row['air_temperature_min_std'],
                'windgust_max_avg': row['windgust_max_avg'],
                'windgust_max_std': row['windgust_max_std'],
                'windgust_mean_avg': row['windgust_mean_avg'],
                'windgust_mean_std': row['windgust_mean_std'],
                'windgust_min_avg': row['windgust_min_avg'],
                'windgust_min_std': row['windgust_min_std'],
                'windspeed_max_avg': row['windspeed_max_avg'],
                'windspeed_max_std': row['windspeed_max_std'],
                'windspeed_mean_avg': row['windspeed_mean_avg'],
                'windspeed_mean_std': row['windspeed_mean_std'],
                'windspeed_min_avg': row['windspeed_min_avg'],
                'windspeed_min_std': row['windspeed_min_std']
            }
            self.db.upsert_weather_data(**upsert_obj)
