import os
import sys
from abc import ABC, abstractmethod

from utils.adapter_abstract import AbstractAdapter

__all__ = ('AbstractFetcher',)

from utils.helpers import AdmTranslator


class AbstractFetcher(ABC):

    def __init__(self, db: AbstractAdapter):
        self.adm_translator = self.load_adm_translator()
        self.db = db

    def load_adm_translator(self):
        translation_csv_fname = getattr(self.__class__, 'TRANSLATION_CSV', "translation.csv")
        path = os.path.dirname(sys.modules[self.__class__.__module__].__file__)
        return AdmTranslator(os.path.join(path, translation_csv_fname))

    @abstractmethod
    def run(self):
        pass
