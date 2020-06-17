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

import os
import logging
import pandas as pd
from datetime import date

__all__ = ('CSVFileHelper',)

from utils.abstract_adapter import AbstractAdapter

logger = logging.getLogger(__name__)

colnames = {
    'government_response': [
        'source', 'date', 'gid', 'country', 'countrycode', 'adm_area_1', 'adm_area_2', 'adm_area_3',
        'c1_school_closing', 'c1_flag',
        'c2_workplace_closing', 'c2_flag',
        'c3_cancel_public_events', 'c3_flag',
        'c4_restrictions_on_gatherings', 'c4_flag',
        'c5_close_public_transport', 'c5_flag',
        'c6_stay_at_home_requirements', 'c6_flag',
        'c7_restrictions_on_internal_movement', 'c7_flag',
        'c8_international_travel_controls',
        'e1_income_support', 'e1_flag',
        'e2_debtcontract_relief',
        'e3_fiscal_measures',
        'e4_international_support',
        'h1_public_information_campaigns', 'h1_flag',
        'h2_testing_policy',
        'h3_contact_tracing',
        'h4_emergency_investment_in_healthcare',
        'h5_investment_in_vaccines',
        'm1_wildcard',
        'stringency_index',
        'stringency_indexfordisplay',
        'stringency_legacy_index',
        'stringency_legacy_indexfordisplay',
        'government_response_index',
        'government_response_index_for_display',
        'containment_health_index',
        'containment_health_index_for_display',
        'economic_support_index',
        'economic_support_index_for_display'
    ],
    'epidemiology': [
        'source', 'date', 'country', 'countrycode', 'adm_area_1',
        'adm_area_2', 'adm_area_3', 'gid', 'tested', 'confirmed',
        'recovered', 'dead', 'hospitalised', 'hospitalised_icu', 'quarantined'
    ],
    'mobility': [
        'source', 'date', 'country', 'countrycode', 'adm_area_1',
        'adm_area_2', 'adm_area_3', 'gid', 'transit_stations', 'residential',
        'workplace', 'parks', 'retail_recreation', 'grocery_pharmacy'
    ],
    'weather': [
        'date', 'countrycode', 'gid', 'precip_max_avg', 'precip_max_std',
        'precip_mean_avg', 'precip_mean_std', 'specific_humidity_max_avg', 'specific_humidity_max_std',
        'specific_humidity_mean_avg', 'specific_humidity_mean_std', 'specific_humidity_min_avg',
        'specific_humidity_min_std', 'short_wave_radiation_max_avg', 'short_wave_radiation_max_std',
        'short_wave_radiation_mean_avg', 'short_wave_radiation_mean_std', 'air_temperature_max_avg',
        'air_temperature_max_std', 'air_temperature_mean_avg', 'air_temperature_mean_std',
        'air_temperature_min_avg', 'air_temperature_min_std',
        'windgust_max_avg', 'windgust_max_std', 'windgust_mean_avg', 'windgust_mean_std', 'windgust_min_avg',
        'windgust_min_std', 'windspeed_max_avg', 'windspeed_max_std', 'windspeed_mean_avg',
        'windspeed_mean_std', 'windspeed_min_avg', 'windspeed_min_std'
    ]
}


class CSVFileHelper(AbstractAdapter):
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.csv_file_name = None
        self.temp_df = None

    def upsert_temp_df(self, csv_file_name: str, data_type: str, data: dict):
        if self.csv_file_name != csv_file_name:
            self.flush()
            self.csv_file_name = csv_file_name
            self.temp_df = pd.DataFrame(columns=colnames.get(data_type))

        # check if row exists
        row = self.temp_df[
            (self.temp_df.date == data.get('date')) & \
            (self.temp_df.countrycode == data.get('countrycode')) & \
            (self.temp_df.adm_area_1 == data.get('adm_area_1')) & \
            (self.temp_df.adm_area_2 == data.get('adm_area_2')) & \
            (self.temp_df.adm_area_3 == data.get('adm_area_3'))
            ].index.tolist()

        if row:
            series = self.temp_df.iloc[row[0]]
            for key, value in data.items():
                series[key] = value
        else:
            self.temp_df = self.temp_df.append(data, ignore_index=True)

    def format_data(self, data):
        if isinstance(data.get('date'), pd.Timestamp):
            data['date'] = data.get('date').date()
        if isinstance(data.get('date'), date):
            data['date'] = data.get('date').strftime("%Y-%m-%d")

        data['gid'] = ":".join(data.get('gid', [])) if data.get('gid') else None
        return data

    def get_adm_division(self, countrycode: str, adm_area_1: str = None, adm_area_2: str = None,
                         adm_area_3: str = None):
        # TODO: Implement get adm division
        raise NotImplementedError("To be implemented")

    def upsert_table_data(self, table_name: str, **kwargs):
        self.check_if_gid_exists(kwargs)
        csv_file_name = f'{table_name}_{kwargs.get("source")}.csv'
        kwargs = self.format_data(kwargs)
        self.upsert_temp_df(csv_file_name, table_name, kwargs)
        logger.debug("Updating {} table with data: {}".format(table_name, list(kwargs.values())))

    def upsert_government_response_data(self, table_name: str = 'government_response', **kwargs):
        self.upsert_table_data(table_name, **kwargs)

    def upsert_epidemiology_data(self, table_name: str = 'epidemiology', **kwargs):
        self.upsert_table_data(table_name, **kwargs)

    def upsert_mobility_data(self, table_name: str = 'mobility', **kwargs):
        self.upsert_table_data(table_name, **kwargs)

    def upsert_weather_data(self, table_name: str = 'weather', **kwargs):
        self.upsert_table_data(table_name, **kwargs)

    def flush(self):
        if self.csv_file_name and self.temp_df is not None:
            csv_file_path = os.path.join(self.csv_path, self.csv_file_name)
            self.temp_df.to_csv(csv_file_path, index=False, header=True)
            logger.debug(f"Saving to CSV {csv_file_path}")
        self.csv_file_name = None
        self.temp_df = None
