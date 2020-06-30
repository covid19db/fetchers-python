from datetime import datetime
from utils.fetcher.abstract_fetcher import AbstractFetcher
from utils.config import config

__all__ = ('Diagnostics',)


class Diagnostics:

    def __init__(self, fetcher_instance: AbstractFetcher):
        self.fetcher_instance = fetcher_instance

    def update_diagnostics_info(self, validation: bool, error: bool, start_time, end_time):
        if config.CSV:
            pass
        else:
            data = {
                "table_name": self.fetcher_instance.TYPE.value,
                "source": self.fetcher_instance.SOURCE,
                "validation_success": validation,
                "error": error,
                "last_run_start": datetime.fromtimestamp(start_time),
                "last_run_stop": datetime.fromtimestamp(end_time),
                "first_timestamp": self.fetcher_instance.get_earliest_timestamp(),
                "last_timestamp": self.fetcher_instance.get_latest_timestamp()
            }
            data_adapter = self.fetcher_instance.data_adapter
            data_adapter.upsert_diagnostics(**data)
