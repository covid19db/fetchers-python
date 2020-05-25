import time
import logging
from typing import Tuple
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from utils.adapter_abstract import AbstractAdapter

MAX_ATTEMPT_FAIL = 10

__all__ = ('PostgresqlHelper',)

logger = logging.getLogger(__name__)


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

    def get_adm_division(self, countrycode: str, adm_area_1: str = None, adm_area_2: str = None,
                         adm_area_3: str = None) -> Tuple:
        sql_query = sql.SQL("""
            SELECT adm_area_1, adm_area_2, adm_area_3, gid from administrative_division
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
        return result['adm_area_1'], result['adm_area_2'], result['adm_area_3'], [result['gid']]

    def upsert_government_response_data(self, table_name: str = 'government_response', **kwargs):
        data_keys = ['gid', 'confirmed', 'dead', 'stringency', 'stringency_actual']

        self.check_if_gid_exists(kwargs)
        sql_query = sql.SQL("""INSERT INTO {table_name} ({insert_keys}) VALUES ({insert_data})
                                    ON CONFLICT
                                        (date, country, countrycode, COALESCE(adm_area_1, ''), COALESCE(adm_area_2, ''), 
                                         COALESCE(adm_area_3, ''), source)
                                    DO
                                        UPDATE SET {update_data}
                                    RETURNING *
                                    """).format(
            table_name=sql.Identifier(table_name),
            insert_keys=sql.SQL(",").join(map(sql.Identifier, kwargs.keys())),
            insert_data=sql.SQL(",").join(map(sql.Placeholder, kwargs.keys())),
            update_data=sql.SQL(",").join(
                sql.Composed([sql.Identifier(k), sql.SQL("="), sql.Placeholder(k)]) for k in kwargs.keys() if
                k in data_keys)
        )

        self.execute(sql_query, kwargs)
        logger.debug("Updating {} table with data: {}".format(table_name, list(kwargs.values())))

    def upsert_epidemiology_data(self, table_name: str = 'epidemiology', **kwargs):
        data_keys = ['gid', 'tested', 'confirmed', 'quarantined', 'hospitalised', 'hospitalised_icu', 'dead',
                     'recovered']

        self.check_if_gid_exists(kwargs)
        sql_query = sql.SQL("""INSERT INTO {table_name} ({insert_keys}) VALUES ({insert_data})
                                ON CONFLICT
                                    (date, country, countrycode, COALESCE(adm_area_1, ''), COALESCE(adm_area_2, ''), 
                                     COALESCE(adm_area_3, ''), source)
                                DO
                                    UPDATE SET {update_data}
                                RETURNING *
                                """).format(
            table_name=sql.Identifier(table_name),
            insert_keys=sql.SQL(",").join(map(sql.Identifier, kwargs.keys())),
            insert_data=sql.SQL(",").join(map(sql.Placeholder, kwargs.keys())),
            update_data=sql.SQL(",").join(
                sql.Composed([sql.Identifier(k), sql.SQL("="), sql.Placeholder(k)]) for k in kwargs.keys() if
                k in data_keys)
        )

        self.execute(sql_query, kwargs)
        logger.debug("Updating {} table with data: {}".format(table_name, list(kwargs.values())))

    def upsert_mobility_data(self, table_name: str = 'mobility', **kwargs):
        data_keys = ['gid', 'transit_stations', 'residential', 'workplace', 'parks', 'retail_recreation',
                     'grocery_pharmacy']

        self.check_if_gid_exists(kwargs)
        sql_query = sql.SQL("""INSERT INTO {table_name} ({insert_keys}) VALUES ({insert_data})
                                    ON CONFLICT
                                        (source, date, country, countrycode, COALESCE(adm_area_1, ''), 
                                         COALESCE(adm_area_2, ''), COALESCE(adm_area_3, ''))
                                    DO
                                        UPDATE SET {update_data}
                                    RETURNING *
                                    """).format(
            table_name=sql.Identifier(table_name),
            insert_keys=sql.SQL(",").join(map(sql.Identifier, kwargs.keys())),
            insert_data=sql.SQL(",").join(map(sql.Placeholder, kwargs.keys())),
            update_data=sql.SQL(",").join(
                sql.Composed([sql.Identifier(k), sql.SQL("="), sql.Placeholder(k)]) for k in kwargs.keys() if
                k in data_keys)
        )

        self.execute(sql_query, kwargs)
        logger.debug(
            "Updating {} table with data: {}".format(table_name, list(kwargs.values())))

    def close_connection(self):
        if self.conn:
            if self.cur:
                self.cur.close()
            self.conn.close()
            logger.debug("Closing connection")
        self.conn = None
        self.cur = None
