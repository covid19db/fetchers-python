import logging
import sqlite3
from typing import Dict
import pandas as pd

__all__ = ('SqliteHelper',)

from utils.adapter_abstract import AbstractAdapter

logger = logging.getLogger(__name__)

sql_create_epidemiology_table = """ 
    CREATE TABLE IF NOT EXISTS epidemiology (
        source text NOT NULL,
        date date NOT NULL,
        country text NOT NULL,
        countrycode text NOT NULL, 
        adm_area_1 text DEFAULT NULL,
        adm_area_2 text DEFAULT NULL,
        adm_area_3 text DEFAULT NULL,
        gid text DEFAULT NULL,
        tested integer DEFAULT NULL,
        confirmed integer DEFAULT NULL,
        recovered integer DEFAULT NULL,
        dead integer DEFAULT NULL,
        hospitalised integer DEFAULT NULL,
        hospitalised_icu integer DEFAULT NULL,
        quarantined integer DEFAULT NULL,
        UNIQUE (source, date, country, countrycode, adm_area_1, adm_area_2, adm_area_3) ON CONFLICT REPLACE,
        PRIMARY KEY (source, date, country, countrycode, adm_area_1, adm_area_2, adm_area_3)
    ) WITHOUT ROWID"""

sql_create_government_response_table = """ 
    CREATE TABLE IF NOT EXISTS government_response (
        source text NOT NULL,
        date date NOT NULL,
        country text NOT NULL,
        countrycode text NOT NULL, 
        adm_area_1 text DEFAULT NULL,
        adm_area_2 text DEFAULT NULL,
        adm_area_3 text DEFAULT NULL,
        gid text DEFAULT NULL,
        confirmed integer DEFAULT NULL,
        dead integer DEFAULT NULL,
        stringency integer DEFAULT NULL,
        stringency_actual integer DEFAULT NULL,
        actions json DEFAULT NULL,
        UNIQUE (source, date, country, countrycode, adm_area_1, adm_area_2, adm_area_3) ON CONFLICT REPLACE,
        PRIMARY KEY (source, date, country, countrycode, adm_area_1, adm_area_2, adm_area_3)
    ) WITHOUT ROWID"""


def update_type(val):
    if isinstance(val, pd.Timestamp):
        return val.date()
    return val


class SqliteHelper(AbstractAdapter):
    def __init__(self, sqlite_file_path: str):
        self.sqlite_file_path = sqlite_file_path

        self.conn = None
        self.cur = None
        self.open_connection()
        self.cursor()
        self.create_tables()

    def open_connection(self):
        self.conn = None
        try:
            self.conn = sqlite3.connect(self.sqlite_file_path)
        except Exception as e:
            print(e)

    def create_tables(self):
        self.execute(sql_create_epidemiology_table)
        self.execute(sql_create_government_response_table)

    def cursor(self):
        self.cur = self.conn.cursor()
        return self.cur

    def execute(self, query: str, data: str = None):
        try:
            if data:
                self.cur.execute(query, data)
            else:
                self.cur.execute(query)
            self.conn.commit()
        except Exception as ex:
            print(ex)

        return self.cur.fetchall()

    def format_data(self, data: Dict):
        # Add adm_area values if don't exist
        data['adm_area_1'] = data.get('adm_area_1')
        data['adm_area_2'] = data.get('adm_area_2')
        data['adm_area_3'] = data.get('adm_area_3')
        data['gid'] = ",".join(data.get('gid'))
        return {k: ('' if 'adm' in k and v is None else v) for k, v in data.items()}

    def get_adm_division(self, countrycode: str, adm_area_1: str = None, adm_area_2: str = None,
                         adm_area_3: str = None):
        # TODO: Implement get division
        raise NotImplementedError("To be implemented")

    def upsert_government_response_data(self, table_name: str = 'government_response', **kwargs):
        kwargs = self.format_data(kwargs)
        sql_query = """INSERT OR REPLACE INTO {table_name} ({insert_keys}) VALUES ({insert_data})""".format(
            table_name=table_name,
            insert_keys=",".join([key for key in kwargs.keys()]),
            insert_data=",".join('?' * len(kwargs)),
        )
        self.execute(sql_query, [update_type(val) for val in kwargs.values()])
        logger.debug("Updating {} table with data: {}".format(table_name, list(kwargs.values())))

    def upsert_epidemiology_data(self, table_name: str = 'epidemiology', **kwargs):
        kwargs = self.format_data(kwargs)
        sql_query = """INSERT OR REPLACE INTO {table_name} ({insert_keys}) VALUES ({insert_data})""".format(
            table_name=table_name,
            insert_keys=",".join([key for key in kwargs.keys()]),
            insert_data=",".join('?' * len(kwargs)),
        )

        self.execute(sql_query, [update_type(val) for val in kwargs.values()])
        logger.debug("Updating {} table with data: {}".format(table_name, list(kwargs.values())))

    def upsert_mobility_data(self, table_name: str = 'mobility', **kwargs):
        sql_query = """INSERT OR REPLACE INTO {table_name} ({insert_keys}) VALUES ({insert_data})""".format(
            table_name=table_name,
            insert_keys=",".join([key for key in kwargs.keys()]),
            insert_data=",".join('?' * len(kwargs)),
        )

        self.execute(sql_query, [update_type(val) for val in kwargs.values()])
        logger.debug("Updating {} table with data: {}".format(table_name, list(kwargs.values())))

    def close_connection(self):
        if self.conn:
            if self.cur:
                self.cur.close()
            self.conn.close()
            logger.debug("Closing connection")
        self.conn = None
        self.cur = None
