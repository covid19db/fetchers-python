from abc import ABC, abstractmethod

__all__ = ('AbstractAdapter',)


class AbstractAdapter(ABC):

    def __init__(self):
        pass

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
