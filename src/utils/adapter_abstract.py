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
from typing import List
from abc import ABC, abstractmethod

__all__ = ('AbstractAdapter',)

logger = logging.getLogger(__name__)


class AbstractAdapter(ABC):

    @staticmethod
    def check_if_gid_exists(kwargs: List) -> bool:
        if not kwargs.get('gid'):
            logger.warning(
                f'GID is missing for: {kwargs.get("countrycode")}, {kwargs.get("adm_area_1")}, '
                f'{kwargs.get("adm_area_2")}, {kwargs.get("adm_area_3")}, please correct your data')

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

    def flush(self):
        pass
