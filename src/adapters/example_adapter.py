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

__all__ = ('ExampleHelper',)

from utils.adapter_abstract import AbstractAdapter

logger = logging.getLogger(__name__)


class ExampleHelper(AbstractAdapter):
    def __init__(self):
        pass

    def get_adm_division(self, countrycode: str, adm_area_1: str = None, adm_area_2: str = None,
                         adm_area_3: str = None):
        # TODO: Implement get adm division
        raise NotImplementedError("To be implemented")

    def upsert_government_response_data(self, table_name: str = 'government_response', **kwargs):
        # TODO: Implement Update govtrack data
        raise NotImplementedError("To be implemented")

    def upsert_epidemiology_data(self, table_name: str = 'epidemiology', **kwargs):
        # TODO Implement Update infections data
        raise NotImplementedError("To be implemented")

    def upsert_mobility_data(self, table_name: str = 'mobility', **kwargs):
        # TODO Implement Update mobility data
        raise NotImplementedError("To be implemented")
