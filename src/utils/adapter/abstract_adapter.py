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
from typing import List, Dict
from datetime import datetime
from utils.types import FetcherType
from abc import ABC, abstractmethod
from utils.config import config

__all__ = ('AbstractAdapter',)

logger = logging.getLogger(__name__)


class AbstractAdapter(ABC):
    MISSING_GIDS = set()

    @staticmethod
    def date_in_window(args: Dict) -> bool:
        if not config.SLIDING_WINDOW_DAYS:
            return True

        date = args.get('date')
        if isinstance(date, str):
            date = datetime.strptime(date.split(' ')[0].split('T')[0], '%Y-%m-%d')

        if isinstance(date, datetime):
            days = (datetime.now() - date).days
            if days > config.SLIDING_WINDOW_DAYS:
                return False

        return True

    @staticmethod
    def correct_table_name(table_name: str) -> str:
        if config.VALIDATE_INPUT_DATA and table_name in ['epidemiology']:
            return 'staging_' + table_name

        return table_name

    @abstractmethod
    def upsert_government_response_data(self, table_name: str, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def upsert_epidemiology_data(self, table_name: str, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def upsert_mobility_data(self, table_name: str, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def get_adm_division(self, countrycode: str, adm_area_1: str = None, adm_area_2: str = None,
                         adm_area_3: str = None):
        raise NotImplementedError()

    def check_if_gid_exists(self, kwargs: List) -> bool:
        if not kwargs.get('gid'):
            missing = kwargs.get("source"), kwargs.get("countrycode"), kwargs.get("adm_area_1"), kwargs.get("adm_area_2"), kwargs.get("adm_area_3")
            if missing not in self.MISSING_GIDS:
                self.MISSING_GIDS.add(missing)

    def publish_missing_gids(self):
        if self.MISSING_GIDS:
            for gid in self.MISSING_GIDS:
                logger.warning(f'GID is missing for: {gid}, please correct your data')

    def upsert_data(self, fetcher_type: FetcherType, **kwargs):
        if not self.date_in_window(kwargs):
            return

        table_name = self.correct_table_name(fetcher_type.value)

        if fetcher_type == FetcherType.EPIDEMIOLOGY:
            return self.upsert_epidemiology_data(table_name, **kwargs)
        elif fetcher_type == FetcherType.EPIDEMIOLOGY_MSOA:
            return self.upsert_epidemiology_data(table_name, **kwargs)
        elif fetcher_type == FetcherType.MOBILITY:
            return self.upsert_mobility_data(table_name, **kwargs)
        elif fetcher_type == FetcherType.GOVERNMENT_RESPONSE:
            return self.upsert_government_response_data(table_name, **kwargs)
        elif fetcher_type == FetcherType.WEATHER:
            return self.upsert_weather_data(table_name, **kwargs)
        else:
            raise NotImplementedError()

    def get_latest_timestamp(self, table_name: str, source: str = None):
        raise NotImplementedError()

    def get_details(self, table_name: str, source: str = None):
        raise NotImplementedError()

    def flush(self):
        pass

    def call_db_function_compare(self, source_code: str) -> bool:
        return False

    def call_db_function_send_data(self, source_code: str):
        pass

    def truncate_staging(self):
        pass
