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

from typing import Dict
from datetime import datetime

from utils.adapter_abstract import AbstractAdapter
from utils.config import config


class AdapterWrapper(AbstractAdapter):

    def __init__(self, data_adapter: AbstractAdapter = None):
        self.data_adapter = data_adapter
        self.sliding_window_days = config.SLIDING_WINDOW_DAYS
        table_name_postfix = 'staging_' if config.VALIDATE_INPUT_DATA else None
        self.table_name_postfix = table_name_postfix

    def date_in_window(self, args: Dict) -> bool:
        if not self.sliding_window_days:
            return True

        date = args.get('date')
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y-%m-%d')

        if isinstance(date, datetime):
            days = (datetime.now() - date).days
            if days > self.sliding_window_days:
                return False

        return True

    def call_db_function_compare(self, source_code: str):
        if hasattr(self.data_adapter, 'call_db_function_compare'):
            return self.data_adapter.call_db_function_compare(source_code)
        return False

    def call_db_function_send_data(self, source_code: str):
        if hasattr(self.data_adapter, 'call_db_function_send_data'):
            self.data_adapter.call_db_function_send_data(source_code)

    def truncate_staging(self):
        if hasattr(self.data_adapter, 'truncate_staging'):
            self.data_adapter.truncate_staging()

    def correct_table_name(self, table_name: str) -> str:
        if self.table_name_postfix and table_name in ['epidemiology']:
            return self.table_name_postfix + table_name
        else:
            return table_name

    def upsert_government_response_data(self, table_name: str = 'government_response', **kwargs):
        if not self.date_in_window(kwargs):
            return
        return self.data_adapter.upsert_government_response_data(self.correct_table_name(table_name), **kwargs)

    def upsert_epidemiology_data(self, table_name: str = 'epidemiology', **kwargs):
        if not self.date_in_window(kwargs):
            return
        return self.data_adapter.upsert_epidemiology_data(self.correct_table_name(table_name), **kwargs)

    def upsert_mobility_data(self, table_name: str = 'mobility', **kwargs):
        if not self.date_in_window(kwargs):
            return
        return self.data_adapter.upsert_mobility_data(self.correct_table_name(table_name), **kwargs)

    def upsert_weather_data(self, table_name: str = 'weather', **kwargs):
        if not self.date_in_window(kwargs):
            return
        return self.data_adapter.upsert_weather_data(self.correct_table_name(table_name), **kwargs)

    def get_adm_division(self, countrycode: str, adm_area_1: str = None, adm_area_2: str = None,
                         adm_area_3: str = None):
        return self.data_adapter.get_adm_division(countrycode, adm_area_1, adm_area_2, adm_area_3)

    def flush(self):
        if hasattr(self.data_adapter, 'flush'):
            self.data_adapter.flush()

    def execute(self, query: str, data: str = None):
        return self.data_adapter.execute(query, data)
