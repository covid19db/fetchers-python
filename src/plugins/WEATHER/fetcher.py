# Copyright (C) 2020 University of Oxford
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import pandas as pd
from functools import reduce
import datetime

from utils.fetcher.base_weather import BaseWeatherFetcher
from .utils import create_aggr_df, load_local_data

__all__ = ('METDailyWeatherFetcher',)

logger = logging.getLogger(__name__)


class METDailyWeatherFetcher(BaseWeatherFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'MET'

    def fetch(self, day, weather_indicators, adm_1_info, adm_2_info):
        # creates a dataframe for each variable and merge them
        dfs = []
        for indicator in weather_indicators:
            df = create_aggr_df(indicator, day, weather_indicators,
                                adm_1_info, adm_2_info, logger)
            if df is None:
                return None
            dfs.append(df)

        df_final = reduce(lambda left,right: pd.merge(
            left,right,on=['source', 'date', 'gid',
                           'country', 'countrycode',
                           'adm_area_1','adm_area_2',
                           'adm_area_3',
                           'samplesize']), dfs)
        return df_final

    def get_last_weather_date(self):
        sql = f"SELECT max(date) as date FROM weather"
        date = pd.DataFrame(self.data_adapter.execute(sql), columns=["date"])

        return date.date.values[0] or datetime.datetime.strptime('2020-01-01', "%Y-%m-%d").date()

    def run(self):
        weather_indicators, adm_1_info, adm_2_info = load_local_data()

        # Define date range
        logger.debug("defining date range")
        last_last_weather_date = self.get_last_weather_date()
        start = last_last_weather_date + datetime.timedelta(days=1)
        stop = datetime.datetime.now() - datetime.timedelta(days=1)
        step = datetime.timedelta(days=1)
        date_range = pd.date_range(start, stop, freq=step)

        for day in date_range:
            logger.debug(f"fetching weather data for: {day.strftime('%Y-%m-%d')}")
            new_data = self.fetch(day, weather_indicators, adm_1_info, adm_2_info)
            if new_data is None:
                continue

            for index, row in new_data.iterrows():
                upsert_obj = {
                    'source': row['source'],
                    'date': row['date'],
                    'gid': row['gid'],
                    'country': row['country'],
                    'countrycode': row['countrycode'],
                    'adm_area_1': row['adm_area_1'],
                    'adm_area_2': row['adm_area_2'],
                    'adm_area_3': row['adm_area_3'],
                    'samplesize': row['samplesize'],
                    'precipitation_max_avg': row['precipitation_max_avg'],
                    'precipitation_max_std': row['precipitation_max_std'],
                    'precipitation_mean_avg': row['precipitation_mean_avg'],
                    'precipitation_mean_std': row['precipitation_mean_std'],
                    'humidity_max_avg': row['humidity_max_avg'],
                    'humidity_max_std': row['humidity_max_std'],
                    'humidity_mean_avg': row['humidity_mean_avg'],
                    'humidity_mean_std': row['humidity_mean_std'],
                    'humidity_min_avg': row['humidity_min_avg'],
                    'humidity_min_std': row['humidity_min_std'],
                    'sunshine_max_avg': row['sunshine_max_avg'],
                    'sunshine_max_std': row['sunshine_max_std'],
                    'sunshine_mean_avg': row['sunshine_mean_avg'],
                    'sunshine_mean_std': row['sunshine_mean_std'],
                    'temperature_max_avg': row['temperature_max_avg'],
                    'temperature_max_std': row['temperature_max_std'],
                    'temperature_mean_avg': row['temperature_mean_avg'],
                    'temperature_mean_std': row['temperature_mean_std'],
                    'temperature_min_avg': row['temperature_min_avg'],
                    'temperature_min_std': row['temperature_min_std'],
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
                    'windspeed_min_std': row['windspeed_min_std'],
                    'cloudaltitude_max_avg': row['cloudaltitude_max_avg'],
                    'cloudaltitude_max_std': row['cloudaltitude_max_std'],
                    'cloudaltitude_min_avg': row['cloudaltitude_min_avg'],
                    'cloudaltitude_min_std': row['cloudaltitude_min_std'],
                    'cloudaltitude_mean_avg': row['cloudaltitude_mean_avg'],
                    'cloudaltitude_mean_std': row['cloudaltitude_mean_std'],
                    'cloudfrac_max_avg': row['cloudfrac_max_avg'],
                    'cloudfrac_max_std': row['cloudfrac_max_std'],
                    'cloudfrac_min_avg': row['cloudfrac_min_avg'],
                    'cloudfrac_min_std': row['cloudfrac_min_std'],
                    'cloudfrac_mean_avg': row['cloudfrac_mean_avg'],
                    'cloudfrac_mean_std': row['cloudfrac_mean_std']
                }
                self.upsert_data(**upsert_obj)
