import logging

__all__ = ('ExampleHelper',)

from src.utils.adapter_abstract import AbstractAdapter

logger = logging.getLogger(__name__)


class ExampleHelper(AbstractAdapter):
    def __init__(self):
        pass

    def upsert_government_response_data(self, **kwargs):
        # TODO: Implement Update govtrack data
        pass

    def upsert_epidemiology_data(self, **kwargs):
        # TODO Implement Update infections data
        pass
