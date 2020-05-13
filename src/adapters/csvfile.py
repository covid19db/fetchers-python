import os
import logging
import pandas as pd
from datetime import date

__all__ = ('CSVFileHelper',)

from utils.adapter_abstract import AbstractAdapter

logger = logging.getLogger(__name__)

colnames = {
    'government_response': [
        'source', 'date', 'country', 'countrycode', 'adm_area_1',
        'adm_area_2', 'adm_area_3', 'gid', 'confirmed',
        'dead', 'stringency', 'stringency_actual', 'actions'
    ],
    'epidemiology': [
        'source', 'date', 'country', 'countrycode', 'adm_area_1',
        'adm_area_2', 'adm_area_3', 'gid', 'tested', 'confirmed',
        'recovered', 'dead', 'hospitalised', 'hospitalised_icu', 'quarantined'
    ]
}


class CSVFileHelper(AbstractAdapter):
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.csv_file_name = None
        self.temp_df = None

    def upsert_temp_df(self, csv_file_name: str, data_type: str, data: dict):
        if self.csv_file_name != csv_file_name:
            self.flush()
            self.csv_file_name = csv_file_name
            self.temp_df = pd.DataFrame(columns=colnames.get(data_type))

        # check if row exists
        row = self.temp_df[
            (self.temp_df.date == data.get('date')) & \
            (self.temp_df.countrycode == data.get('countrycode')) & \
            (self.temp_df.adm_area_1 == data.get('adm_area_1')) & \
            (self.temp_df.adm_area_2 == data.get('adm_area_2')) & \
            (self.temp_df.adm_area_3 == data.get('adm_area_3'))
            ].index.tolist()

        if row:
            series = self.temp_df.iloc[row[0]]
            for key, value in data.items():
                series[key] = value
        else:
            self.temp_df.append(data, ignore_index=True)

    def format_data(self, data):
        if isinstance(data.get('date'), pd.Timestamp):
            data['date'] = data.get('date').date()
        if isinstance(data.get('date'), date):
            data['date'] = data.get('date').strftime("%Y-%m-%d")

        data['gid'] = ":".join(data.get('gid', ''))
        return data

    def upsert_government_response_data(self, table_name: str = 'government_response', **kwargs):
        csv_file_name = f'{table_name}_{kwargs.get("source")}.csv'
        kwargs = self.format_data(kwargs)
        self.upsert_temp_df(csv_file_name, table_name, kwargs)
        logger.debug("Updating {} table with data: {}".format(table_name, list(kwargs.values())))

    def upsert_epidemiology_data(self, table_name: str = 'epidemiology', **kwargs):
        csv_file_name = f'{table_name}_{kwargs.get("source")}.csv'
        kwargs = self.format_data(kwargs)
        self.upsert_temp_df(csv_file_name, table_name, kwargs)
        logger.debug("Updating {} table with data: {}".format(table_name, list(kwargs.values())))

    def flush(self):
        if self.csv_file_name and self.temp_df is not None:
            csv_file_path = os.path.join(self.csv_path, self.csv_file_name)
            self.temp_df.to_csv(csv_file_path, index=False, header=True)
            logger.debug(f"Saving to CSV {csv_file_path}")
        self.csv_file_name = None
        self.temp_df = None
