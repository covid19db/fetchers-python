from abc import ABC, abstractmethod

from utils.database import DB

__all__ = ('AbstractFetcher',)


class AbstractFetcher(ABC):

    def __init__(self, db: DB):
        self.db = db

    @abstractmethod
    def run(self):
        pass
