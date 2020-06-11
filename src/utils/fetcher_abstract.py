# Copyright University of Oxford 2020
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
import sys
from enum import Enum
from abc import ABC, abstractmethod

from utils.config import config
from utils.adapter_abstract import AbstractAdapter

__all__ = ('AbstractFetcher', 'FetcherType')

from utils.country_codes_translator.translator import CountryCodesTranslator
from utils.administrative_division_translator.translator import AdmTranslator


class FetcherType(Enum):
    EPIDEMIOLOGY = 'epidemiology'
    GOVERNMENT_RESPONSE = 'government_response'
    MOBILITY = 'mobility'
    WEATHER = 'weather'


class AbstractFetcher(ABC):
    TYPE = FetcherType.EPIDEMIOLOGY
    LOAD_PLUGIN = False

    def __init__(self, db: AbstractAdapter):
        self.adm_translator = self.load_adm_translator()
        self.country_codes_translator = CountryCodesTranslator()
        self.sliding_window_days = config.SLIDING_WINDOW_DAYS
        self.db = db

    def load_adm_translator(self) -> AdmTranslator:
        translation_csv_fname = getattr(self.__class__, 'TRANSLATION_CSV', "translation.csv")
        path = os.path.dirname(sys.modules[self.__class__.__module__].__file__)
        return AdmTranslator(os.path.join(path, translation_csv_fname))

    def get_region(self, countrycode: str, input_adm_area_1: str = None, input_adm_area_2: str = None,
                   input_adm_area_3: str = None, suppress_exception: bool = False):
        try:
            # Check if input data can be matched directly into administrative division table
            adm_area_1, adm_area_2, adm_area_3, gid = self.db.get_adm_division(
                countrycode, input_adm_area_1, input_adm_area_2, input_adm_area_3)
        except Exception as ex:
            adm_area_1, adm_area_2, adm_area_3, gid = None, None, None, None

        if not gid:
            # Check translate.csv for translation
            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                countrycode, input_adm_area_1, input_adm_area_2, input_adm_area_3,
                return_original_if_failure=False,
                suppress_exception=suppress_exception
            )

        return adm_area_1, adm_area_2, adm_area_3, gid

    @abstractmethod
    def run(self):
        pass
