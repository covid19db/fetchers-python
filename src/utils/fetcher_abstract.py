from abc import ABC, abstractmethod

from utils.adapter_abstract import AbstractAdapter

__all__ = ('AbstractFetcher',)


class AbstractFetcher(ABC):

    def __init__(self, db: AbstractAdapter):
        self.db = db

    @abstractmethod
    def run(self):
        pass
