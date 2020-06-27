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

    def fetch(self, day, weather_indicators, adm_2_to_grid):
        # creates a dataframe for each variable and merge them
        dfs = [create_aggr_df(indicator, day, weather_indicators,
                              adm_2_to_grid, logger) for indicator in weather_indicators]
        df_final = reduce(lambda left, right: pd.merge(
            left, right, on=['day', 'country', 'region', 'city']), dfs)
        return df_final

    def get_last_weather_date(self):
        sql = f"SELECT max(date) as date FROM weather"
        date = pd.DataFrame(self.data_adapter.execute(sql), columns=["date"])

        return date.date.values[0] or datetime.datetime.strptime('2020-01-01', "%Y-%m-%d").date()

    def run(self):
        weather_indicators, adm_2_to_grid = load_local_data()

        # Define date range
        logger.debug("defining date range")
        last_last_weather_date = self.get_last_weather_date()
        start = last_last_weather_date + datetime.timedelta(days=1)
        stop = datetime.datetime.now() - datetime.timedelta(days=1)
        step = datetime.timedelta(days=1)
        date_range = pd.date_range(start, stop, freq=step)

        for day in date_range:
            logger.debug(f"fetching weather data for: {day.strftime('%Y-%m-%d')}")
            new_data = self.fetch(day, weather_indicators, adm_2_to_grid)

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
                self.upsert_data(**upsert_obj)
