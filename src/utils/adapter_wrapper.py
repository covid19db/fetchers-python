from typing import Dict
from datetime import datetime

from utils.adapter_abstract import AbstractAdapter


class AdapterWrapper(AbstractAdapter):

    def __init__(self, base_apapter: AbstractAdapter = None,
                 sliding_window_days: bool = None,
                 table_name_postfix: bool = None):
        self.base_adapter = base_apapter
        self.sliding_window_days = int(sliding_window_days) if sliding_window_days else None
        self.table_name_postfix = table_name_postfix

    def date_in_window(self, args: Dict) -> bool:
        if not self.sliding_window_days:
            return True

        date = args.get('date')
        if date and isinstance(date, str):
            date = datetime.strptime(date, '%Y-%m-%d')

        if date and isinstance(date, datetime):
            days = (datetime.now() - date).days
            if days > self.sliding_window_days:
                return False

        return True

    def correct_table_name(self, table_name: str) -> str:
        return table_name + self.table_name_postfix if self.table_name_postfix else table_name

    def upsert_government_response_data(self, table_name: str = 'government_response', **kwargs):
        if not self.date_in_window(kwargs):
            return
        self.base_adapter.upsert_government_response_data(self.correct_table_name(table_name), **kwargs)

    def upsert_epidemiology_data(self, table_name: str = 'epidemiology', **kwargs):
        if not self.date_in_window(kwargs):
            return
        self.base_adapter.upsert_epidemiology_data(self.correct_table_name(table_name), **kwargs)

    def upsert_mobility_data(self, table_name: str = 'mobility', **kwargs):
        if not self.date_in_window(kwargs):
            return
        self.base_adapter.upsert_mobility_data(self.correct_table_name(table_name), **kwargs)

    def get_adm_division(self, countrycode: str, adm_area_1: str = None, adm_area_2: str = None,
                         adm_area_3: str = None):
        self.base_adapter.get_adm_division(countrycode, adm_area_1, adm_area_2, adm_area_3)

    def flush(self):
        if hasattr(self.base_adapter, 'flush'):
            self.base_adapter.flush()
