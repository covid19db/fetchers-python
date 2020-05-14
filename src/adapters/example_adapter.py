import logging

__all__ = ('ExampleHelper',)

from utils.adapter_abstract import AbstractAdapter

logger = logging.getLogger(__name__)


class ExampleHelper(AbstractAdapter):
    def __init__(self):
        pass

    def get_gid(self, countrycode: str, adm_area_1: str = None, adm_area_2: str = None, adm_area_3: str = None):
        # TODO: Implement get gid
        raise NotImplementedError("To be implemented")

    def upsert_government_response_data(self, **kwargs):
        # TODO: Implement Update govtrack data
        raise NotImplementedError("To be implemented")

    def upsert_epidemiology_data(self, **kwargs):
        # TODO Implement Update infections data
        raise NotImplementedError("To be implemented")
