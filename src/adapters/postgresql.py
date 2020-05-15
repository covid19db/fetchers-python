import time
import logging
import functools
import operator
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
    ###########Ahmad Changes 
    def call_db_function_compare(self, source_code: str):
        sql_query =sql.SQL("SELECT covid19_compare_tables('source_code')") 
        compare_result = self.execute(sql_query)
        logger.debug(
            "validating incoming data")
        return compare_result
    
    def call_db_function_send_data(self, source_code: str):
        self.cur.callproc('send_validated_data',[source_code])
        logger.debug(
            "moving data to epidemiology")
        
    def get_souce_code(self, pname: str):
        sql_query = sql.SQL("SELECT source_code from url_info where plugin_name='%s' limit 1" %pname)         
        data=self.execute(sql_query)     
        logger.debug(
            "selecting data from email info")
        return data
     ###########Ahmad Changes 

    def get_gid(self, countrycode: str, adm_area_1: str = None, adm_area_2: str = None, adm_area_3: str = None):
        sql_query = sql.SQL("""SELECT gid from administrative_division
                                WHERE countrycode = %s 
                                AND COALESCE(adm_area_1, '') = COALESCE(%s, '')
                                AND COALESCE(adm_area_2, '') = COALESCE(%s, '') 
                                AND COALESCE(adm_area_3, '') = COALESCE(%s, '') """)

        result = self.execute(sql_query, (countrycode, adm_area_1, adm_area_2, adm_area_3))
        gid = functools.reduce(operator.iconcat, result, [])
        if not gid:
            raise Exception(f'Unable to find GID for: {countrycode} {adm_area_1} {adm_area_2} {adm_area_3}')
        return gid

    def get_administrative_division_for_country(self, countrycode: str, adm_level: str):
        sql_query = sql.SQL("""SELECT country, countrycode, countrycode_alpha2, adm_level,
         adm_area_1, adm_area_2, adm_area_3, gid from administrative_division
                              WHERE countrycode LIKE %s AND adm_leve LIKE %s """)

        result = self.execute(sql_query, (countrycode, adm_level))
        return result

    def upsert_government_response_data(self, table_name: str = 'government_response', **kwargs):
        data_keys = ['gid', 'confirmed', 'dead', 'stringency', 'stringency_actual']

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
        
    ## Ahmad Changes : changed table name to staging_epidemiology
    def upsert_epidemiology_data(self, table_name: str = 'staging_epidemiology', **kwargs):
        data_keys = ['gid', 'tested', 'confirmed', 'quarantined', 'hospitalised', 'hospitalised_icu', 'dead',
                     'recovered']

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
                self.cur.close_connection()
            self.conn.close_connection()
            logger.debug("Closing connection")
        self.conn = None
        self.cur = None
