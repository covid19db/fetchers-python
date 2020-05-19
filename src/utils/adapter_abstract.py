from typing import List
from abc import ABC, abstractmethod

__all__ = ('AbstractAdapter',)


class AbstractAdapter(ABC):

    def __init__(self):
        pass

    @staticmethod
    def check_if_gid_exists(kwargs: List) -> bool:
        if not kwargs.get('gid'):
            raise Exception(
                f'GID is missing for: {kwargs.get("countrycode")}, {kwargs.get("adm_area_1")}, '
                f'{kwargs.get("adm_area_2")}, {kwargs.get("adm_area_3")}, please correct your data')

    @abstractmethod
    def upsert_government_response_data(self, table_name: str, **kwargs):
        pass

    @abstractmethod
    def upsert_epidemiology_data(self, table_name: str, **kwargs):
        pass

    @abstractmethod
    def upsert_mobility_data(self, table_name: str, **kwargs):
        pass

    @abstractmethod
    def get_adm_division(self, countrycode: str, adm_area_1: str = None, adm_area_2: str = None,
                         adm_area_3: str = None):
        pass

    def flush(self):
        pass
