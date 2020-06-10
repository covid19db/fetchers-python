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
        c1_school_closing integer DEFAULT NULL,
        c1_flag integer DEFAULT NULL,
        c2_workplace_closing integer DEFAULT NULL,
        c2_flag integer DEFAULT NULL,
        c3_cancel_public_events integer DEFAULT NULL,
        c3_flag integer DEFAULT NULL,
        c4_restrictions_on_gatherings integer DEFAULT NULL,
        c4_flag integer DEFAULT NULL,
        c5_close_public_transport integer DEFAULT NULL,
        c5_flag integer DEFAULT NULL,
        c6_stay_at_home_requirements integer DEFAULT NULL,
        c6_flag integer DEFAULT NULL,
        c7_restrictions_on_internal_movement integer DEFAULT NULL,
        c7_flag integer DEFAULT NULL,
        c8_international_travel_controls integer DEFAULT NULL,
        e1_income_support integer DEFAULT NULL,
        e1_flag integer DEFAULT NULL,
        e2_debtcontract_relief integer DEFAULT NULL,
        e3_fiscal_measures float DEFAULT NULL,
        e4_international_support float DEFAULT NULL,
        h1_public_information_campaigns integer DEFAULT NULL,
        h1_flag integer DEFAULT NULL,
        h2_testing_policy integer DEFAULT NULL,
        h3_contact_tracing integer DEFAULT NULL,
        h4_emergency_investment_in_healthcare float DEFAULT NULL,
        h5_investment_in_vaccines float DEFAULT NULL,
        m1_wildcard text DEFAULT NULL,
        stringency_index float DEFAULT NULL,
        stringency_indexfordisplay float DEFAULT NULL,
        stringency_legacy_index float DEFAULT NULL,
        stringency_legacy_indexfordisplay float DEFAULT NULL,
        government_response_index float DEFAULT NULL,
        government_response_index_for_display float DEFAULT NULL,
        containment_health_index float DEFAULT NULL,
        containment_health_index_for_display float DEFAULT NULL,
        economic_support_index float DEFAULT NULL,
        economic_support_index_for_display float DEFAULT NULL,
        actions json DEFAULT NULL,
        UNIQUE (source, date, country, countrycode, adm_area_1, adm_area_2, adm_area_3) ON CONFLICT REPLACE,
        PRIMARY KEY (source, date, country, countrycode, adm_area_1, adm_area_2, adm_area_3)
    ) WITHOUT ROWID"""

sql_create_mobility_table = """
    CREATE TABLE IF NOT EXISTS government_response (
        source text NOT NULL,
        date date NOT NULL,
        country text NOT NULL,
        countrycode text NOT NULL,
        adm_area_1 text DEFAULT NULL,
        adm_area_2 text DEFAULT NULL,
        adm_area_3 text DEFAULT NULL,
        gid text DEFAULT NULL,
        transit_stations integer DEFAULT NULL,
        residential integer DEFAULT NULL,
        workplace  integer DEFAULT NULL,
        parks  integer DEFAULT NULL,
        retail_recreation integer DEFAULT NULL,
        grocery_pharmacy integer DEFAULT NULL,
        transit integer DEFAULT NULL,
        walking integer DEFAULT NULL,
        driving integer DEFAULT NULL,
        UNIQUE (source, date, country, countrycode, adm_area_1, adm_area_2, adm_area_3) ON CONFLICT REPLACE,
        PRIMARY KEY (source, date, country, countrycode, adm_area_1, adm_area_2, adm_area_3)
    ) WITHOUT ROWID"""

sql_create_weather_table = """
    CREATE TABLE IF NOT EXISTS weather (
        date date NOT NULL,
        countrycode text NOT NULL,
        gid text NOT NULL,
        precip_max_avg float,
        precip_max_std float,
        precip_mean_avg float,
        precip_mean_std float,
        specific_humidity_max_avg float,
        specific_humidity_max_std float,
        specific_humidity_mean_avg float,
        specific_humidity_mean_std float,
        specific_humidity_min_avg float,
        specific_humidity_min_std float,
        short_wave_radiation_max_avg float,
        short_wave_radiation_max_std float,
        short_wave_radiation_mean_avg float,
        short_wave_radiation_mean_std float,
        air_temperature_max_avg float,
        air_temperature_max_std float,
        air_temperature_mean_avg float,
        air_temperature_mean_std float,
        air_temperature_min_avg float,
        air_temperature_min_std float,
        windgust_max_avg float,
        windgust_max_std float,
        windgust_mean_avg float,
        windgust_mean_std float,
        windgust_min_avg float,
        windgust_min_std float,
        windspeed_max_avg float,
        windspeed_max_std float,
        windspeed_mean_avg float,
        windspeed_mean_std float,
        windspeed_min_avg float,
        windspeed_min_std float,
        UNIQUE (gid, date),
        PRIMARY KEY (gid, date)
    )"""


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
        self.execute(sql_create_mobility_table)
        self.execute(sql_create_weather_table)

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

    def upsert_data(self, table_name: str, **kwargs):
        self.check_if_gid_exists(kwargs)
        kwargs = self.format_data(kwargs)
        sql_query = """INSERT OR REPLACE INTO {table_name} ({insert_keys}) VALUES ({insert_data})""".format(
            table_name=table_name,
            insert_keys=",".join([key for key in kwargs.keys()]),
            insert_data=",".join('?' * len(kwargs)),
        )
        self.execute(sql_query, [update_type(val) for val in kwargs.values()])
        logger.debug("Updating {} table with data: {}".format(table_name, list(kwargs.values())))

    def upsert_government_response_data(self, table_name: str = 'government_response', **kwargs):
        self.upsert_data(table_name, **kwargs)

    def upsert_epidemiology_data(self, table_name: str = 'epidemiology', **kwargs):
        self.upsert_data(table_name, **kwargs)

    def upsert_mobility_data(self, table_name: str = 'mobility', **kwargs):
        self.upsert_data(table_name, **kwargs)

    def upsert_weather_data(self, table_name: str = 'weather', **kwargs):
        self.upsert_data(table_name, **kwargs)

    def close_connection(self):
        if self.conn:
            if self.cur:
                self.cur.close()
            self.conn.close()
            logger.debug("Closing connection")
        self.conn = None
        self.cur = None
