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

    def __init__(self, db: AbstractAdapter):
        self.adm_translator = self.load_adm_translator()
        self.country_codes_translator = self.load_country_codes_translator()
        self.sliding_window_days = config.SLIDING_WINDOW_DAYS
        self.db = db

    def load_adm_translator(self) -> AdmTranslator:
        translation_csv_fname = getattr(self.__class__, 'TRANSLATION_CSV', "translation.csv")
        path = os.path.dirname(sys.modules[self.__class__.__module__].__file__)
        return AdmTranslator(os.path.join(path, translation_csv_fname))

    def load_country_codes_translator(self) -> CountryCodesTranslator:
        return CountryCodesTranslator()

    @abstractmethod
    def run(self):
        pass
