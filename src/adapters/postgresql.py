# Copyright (C) 2020 University of Oxford
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import json
import datetime
import logging
from typing import Tuple, List
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from utils.adapter.abstract_adapter import AbstractAdapter

MAX_ATTEMPT_FAIL = 10

__all__ = ('PostgresqlHelper',)

logger = logging.getLogger(__name__)


def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()


class PostgresqlHelper(AbstractAdapter):
    def __init__(self, user: str, password: str, host: str, port: str, database_name: str):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database_name = database_name

        self.conn = None
        self.cur = None
        self.open_connection()
        self.cursor()

    def reset_connection(self):
        self.close_connection()
        self.open_connection()
        self.cursor()

    def open_connection(self, attempt: int = MAX_ATTEMPT_FAIL):
        if not self.conn:
            try:
                self.conn = psycopg2.connect(user=self.user, password=self.password, host=self.host,
                                             port=self.port, database=self.database_name, connect_timeout=5)
                self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            except psycopg2.OperationalError as error:
                if attempt > 0:
                    logger.error(f"Got error: {error}, reconnecting")
                    time.sleep(5)
                    self.open_connection(attempt - 1)
                else:
                    raise error
            except (Exception, psycopg2.Error) as error:
                raise error

    def cursor(self):
        if not self.cur or self.cur.closed:
            if not self.conn:
                self.open_connection()
            self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            return self.cur

    def execute(self, query: str, data: str = None, attempt: int = MAX_ATTEMPT_FAIL):
        try:
            self.cur.execute(query, data)
            self.conn.commit()
        except (psycopg2.DatabaseError, psycopg2.OperationalError) as error:
            if attempt > 0:
                logger.error(f"Got error: {error}, retrying")
                time.sleep(1)
                self.reset_connection()
                self.execute(query, data, attempt - 1)
            else:
                raise error
        except (Exception, psycopg2.Error) as error:
            raise error
        return self.cur.fetchall()

    def call_db_function_compare(self, source_code: str) -> int:
        self.cur.callproc('covid19_compare_tables', (source_code,))
        logger.debug("Validating incoming data...")
        compare_result = self.cur.fetchone()
        return compare_result[0]

    def call_db_function_send_data(self, source_code: str):
        self.cur.callproc('send_validated_data', [source_code])
        logger.debug("Moving data to epidemiology")

    def truncate_staging(self):
        # TODO: Add more staging tables, currently only for epidemiology
        sql_query = sql.SQL("""TRUNCATE staging_epidemiology; SELECT 1""")
        self.execute(sql_query)

    def get_adm_division(self, countrycode: str, adm_area_1: str = None, adm_area_2: str = None,
                         adm_area_3: str = None) -> Tuple:
        sql_query = sql.SQL("""
            SELECT country, adm_area_1, adm_area_2, adm_area_3, gid from administrative_division
            WHERE countrycode = %s
                AND regexp_replace(COALESCE(adm_area_1, ''), '[^\w%%]+','','g')
                    ILIKE regexp_replace(%s, '[^\w%%]+','','g')
                AND regexp_replace(COALESCE(adm_area_2, ''), '[^\w%%]+','','g')
                    ILIKE regexp_replace(%s, '[^\w%%]+','','g')
                AND regexp_replace(COALESCE(adm_area_3, ''), '[^\w%%]+','','g')
                    ILIKE regexp_replace(%s, '[^\w%%]+','','g') """)

        results = self.execute(sql_query, (countrycode, adm_area_1 or '', adm_area_2 or '', adm_area_3 or ''))
        if not results:
            raise Exception(f'Unable to find adm division for: {countrycode}, {adm_area_1}, {adm_area_2}, {adm_area_3}')
        if len(results) > 1:
            raise Exception(f'Ambiguous result: {results}')
        result = results[0]
        return result['country'], result['adm_area_1'], result['adm_area_2'], result['adm_area_3'], [result['gid']]

    def upsert_table_data(self, table_name: str, data_keys: List, **kwargs):
        self.check_if_gid_exists(kwargs)
        target = ["date", "country", "countrycode", "COALESCE(adm_area_1, '')", "COALESCE(adm_area_2, '')",
                  "COALESCE(adm_area_3, '')", "source"]

        if 'msoa' in data_keys:
            target.append('msoa')

        sql_query = sql.SQL("""INSERT INTO {table_name} ({insert_keys}) VALUES ({insert_data})
                                ON CONFLICT
                                    (""" + ",".join(target) + """)
                                DO
                                    UPDATE SET {update_data}
                                RETURNING *""").format(
            table_name=sql.Identifier(table_name),
            insert_keys=sql.SQL(",").join(map(sql.Identifier, kwargs.keys())),
            insert_data=sql.SQL(",").join(map(sql.Placeholder, kwargs.keys())),
            update_data=sql.SQL(",").join(
                sql.Composed([sql.Identifier(k), sql.SQL("="), sql.Placeholder(k)]) for k in kwargs.keys() if
                k in data_keys)
        )

        self.execute(sql_query, kwargs)
        logger.debug("Updating {} table with data: {}".format(table_name, list(kwargs.values())))

    def upsert_government_response_data(self, table_name: str = 'government_response', **kwargs):
        government_response_data_fields = [
            'c1_school_closing', 'c1_flag',
            'c2_workplace_closing', 'c2_flag',
            'c3_cancel_public_events', 'c3_flag',
            'c4_restrictions_on_gatherings', 'c4_flag',
            'c5_close_public_transport', 'c5_flag',
            'c6_stay_at_home_requirements', 'c6_flag',
            'c7_restrictions_on_internal_movement', 'c7_flag',
            'c8_international_travel_controls',
            'e1_income_support', 'e1_flag',
            'e2_debtcontract_relief',
            'e3_fiscal_measures',
            'e4_international_support',
            'h1_public_information_campaigns', 'h1_flag',
            'h2_testing_policy',
            'h3_contact_tracing',
            'h4_emergency_investment_in_healthcare',
            'h5_investment_in_vaccines',
            'm1_wildcard',
            'stringency_index',
            'stringency_indexfordisplay',
            'stringency_legacy_index',
            'stringency_legacy_indexfordisplay',
            'government_response_index',
            'government_response_index_for_display',
            'containment_health_index',
            'containment_health_index_for_display',
            'economic_support_index',
            'economic_support_index_for_display',
            'actions'
        ]
        self.upsert_table_data(table_name, government_response_data_fields, **kwargs)

    def upsert_epidemiology_data(self, table_name: str = 'epidemiology', data_keys: list = None, **kwargs):
        if not data_keys:
            data_keys = ['gid', 'tested', 'confirmed', 'quarantined', 'hospitalised', 'hospitalised_icu', 'dead',
                         'recovered']
        self.upsert_table_data(table_name, data_keys, **kwargs)

    def upsert_mobility_data(self, table_name: str = 'mobility', **kwargs):
        data_keys = ['gid', 'transit_stations', 'residential', 'workplace', 'parks', 'retail_recreation',
                     'grocery_pharmacy']

        self.upsert_table_data(table_name, data_keys, **kwargs)

    def upsert_weather_data(self, table_name: str = 'weather', **kwargs):
        composite_key = ['date', 'countrycode', 'gid']

        self.check_if_gid_exists(kwargs)
        sql_query = sql.SQL("""INSERT INTO {table_name} ({insert_keys}) VALUES ({insert_data})
                                ON CONFLICT
                                    (date, gid)
                                DO
                                    UPDATE SET {update_data}
                               RETURNING *
                                    """).format(
            table_name=sql.Identifier(table_name),
            insert_keys=sql.SQL(",").join(map(sql.Identifier, kwargs.keys())),
            insert_data=sql.SQL(",").join(map(sql.Placeholder, kwargs.keys())),
            update_data=sql.SQL(",").join(
                sql.Composed([sql.Identifier(k), sql.SQL("="), sql.Placeholder(k)]) for k in kwargs.keys() if
                k not in composite_key)
        )

        self.execute(sql_query, kwargs)
        logger.debug(
            "Updating {} table with data: {}".format(table_name, list(kwargs.values())))

    def upsert_diagnostics(self, **kwargs):
        data_keys = ["validation_success", "error", "last_run_start", "last_run_stop", "first_timestamp",
                     "last_timestamp", "details"]
        sql_query = sql.SQL("""INSERT INTO diagnostics ({insert_keys}) VALUES ({insert_data})
                                        ON CONFLICT
                                            (table_name, source)
                                        DO
                                            UPDATE SET {update_data}
                                        RETURNING *""").format(
            insert_keys=sql.SQL(",").join(map(sql.Identifier, kwargs.keys())),
            insert_data=sql.SQL(",").join(map(sql.Placeholder, kwargs.keys())),
            update_data=sql.SQL(",").join(
                sql.Composed([sql.Identifier(k), sql.SQL("="), sql.Placeholder(k)]) for k in kwargs.keys() if
                k in data_keys)
        )

        self.execute(sql_query, kwargs)
        logger.debug("Updating diagnostics table with data: {}".format(list(kwargs.values())))

    def get_earliest_timestamp(self, table_name: str, source: str = None):
        sql_str = """SELECT min(date) as date FROM {table_name}"""
        if source:
            sql_str = sql_str + """ WHERE source = %s"""

        sql_query = sql.SQL(sql_str).format(table_name=sql.Identifier(table_name))

        result = self.execute(sql_query, (source,))
        return result[0]['date'] if len(result) > 0 else None

    def get_latest_timestamp(self, table_name: str, source: str = None):
        sql_str = """SELECT max(date) as date FROM {table_name}"""
        if source:
            sql_str = sql_str + """ WHERE source = %s"""

        sql_query = sql.SQL(sql_str).format(table_name=sql.Identifier(table_name))

        result = self.execute(sql_query, (source,))
        return result[0]['date'] if len(result) > 0 else None

    def get_details(self, table_name: str, source: str = None):
        sql_str = """SELECT country, min(date) as min_date, max(date) as max_date  
                     FROM {table_name}"""
        if source:
            sql_str = sql_str + """ WHERE source = %s"""
        sql_str = sql_str + " GROUP BY country"

        sql_query = sql.SQL(sql_str).format(table_name=sql.Identifier(table_name))

        result = self.execute(sql_query, (source,))
        result_list = []
        columns = ['country', 'min_date', 'max_date']
        for row in result:
            result_list.append(dict(zip(columns, row)))
        return json.dumps(result_list, default=default)

    def close_connection(self):
        if self.conn:
            if self.cur:
                self.cur.close()
            self.conn.close()
            logger.debug("Closing connection")
        self.conn = None
        self.cur = None
