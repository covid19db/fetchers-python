from abc import ABC, abstractmethod

__all__ = ('AbstractAdapter',)


class AbstractAdapter(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def upsert_government_response_data(self, **kwargs):
        pass

    @abstractmethod
    def upsert_epidemiology_data(self, **kwargs):
        pass

    @abstractmethod
    def get_gid(self, countrycode: str, adm_area_1: str = None, adm_area_2: str = None, adm_area_3: str = None):
        pass

    def flush(self):
        pass
