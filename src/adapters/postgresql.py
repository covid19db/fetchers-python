import time
import logging
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

    def open_connection(self, attempt=MAX_ATTEMPT_FAIL):
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

    def execute(self, query: str, data: str = None, attempt=MAX_ATTEMPT_FAIL):
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

    def upsert_government_response_data(self, **kwargs):
        data_keys = ['confirmed', 'dead', 'stringency', 'stringency_actual']

        sql_query = sql.SQL("""INSERT INTO government_response ({insert_keys}) VALUES ({insert_data})
                                    ON CONFLICT
                                        (date, country, countrycode, COALESCE(adm_area_1, ''), COALESCE(adm_area_2, ''), 
                                         COALESCE(adm_area_3, ''), source)
                                    DO
                                        UPDATE SET {update_data}
                                    RETURNING *
                                    """).format(
            insert_keys=sql.SQL(",").join(map(sql.Identifier, kwargs.keys())),
            insert_data=sql.SQL(",").join(map(sql.Placeholder, kwargs.keys())),
            update_data=sql.SQL(",").join(
                sql.Composed([sql.Identifier(k), sql.SQL("="), sql.Placeholder(k)]) for k in kwargs.keys() if
                k in data_keys)
        )

        self.execute(sql_query, kwargs)
        logger.debug(
            "Updating govtrack table with data: {}".format([kwargs[k] for k in kwargs.keys() if k not in data_keys]))

    def upsert_epidemiology_data(self, **kwargs):
        data_keys = ['tested', 'confirmed', 'quarantined', 'hospitalised', 'hospitalised_icu', 'dead', 'recovered']

        sql_query = sql.SQL("""INSERT INTO epidemiology ({insert_keys}) VALUES ({insert_data})
                                ON CONFLICT
                                    (date, country, countrycode, COALESCE(adm_area_1, ''), COALESCE(adm_area_2, ''), 
                                     COALESCE(adm_area_3, ''), source)
                                DO
                                    UPDATE SET {update_data}
                                RETURNING *
                                """).format(
            insert_keys=sql.SQL(",").join(map(sql.Identifier, kwargs.keys())),
            insert_data=sql.SQL(",").join(map(sql.Placeholder, kwargs.keys())),
            update_data=sql.SQL(",").join(
                sql.Composed([sql.Identifier(k), sql.SQL("="), sql.Placeholder(k)]) for k in kwargs.keys() if
                k in data_keys)
        )

        self.execute(sql_query, kwargs)
        logger.debug(
            "Updating infections table with data: {}".format([kwargs[k] for k in kwargs.keys() if k not in data_keys]))
			
    def select_data(self, tname: str):
        sql_query = sql.SQL("select * from %s" %tname)   
        
        data=self.execute(sql_query,"None")
        logger.debug(
            "selecting data from %s ", tname)
        return data
        
    def delete_data(self, tname: str):
        
        sql_query = sql.SQL("delete from %s returning source" %tname)
        # sql_query = sql.SQL("delete from %s where source='USA_NYT' and confirmed < 2 and adm_area_1 ='Nebraska' returning source" %tname)
        
        self.execute(sql_query,"None")
        # print(data)
        logger.debug(
            "Deleting data from %s ", tname)
    
    def select_table_header(self, tname: str):
        sql_query = sql.SQL("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE") 
        sql_query += "table_name = '{}';".format( tname )
        
        
        data=self.execute(sql_query)
        print(data)
        logger.debug(
            "selecting data from %s ", tname)
        return data
    
    def upsert_staging_epidemiology_data(self, **kwargs):
        data_keys = ['tested', 'confirmed', 'quarantined', 'hospitalised', 'hospitalised_icu', 'dead', 'recovered']

        sql_query = sql.SQL("""INSERT INTO staging_epidemiology ({insert_keys}) VALUES ({insert_data})
                                ON CONFLICT
                                    (date, country, countrycode, COALESCE(adm_area_1, ''), COALESCE(adm_area_2, ''), 
                                     COALESCE(adm_area_3, ''), source)
                                DO
                                    UPDATE SET {update_data}
                                RETURNING *
                                """).format(
            insert_keys=sql.SQL(",").join(map(sql.Identifier, kwargs.keys())),
            insert_data=sql.SQL(",").join(map(sql.Placeholder, kwargs.keys())),
            update_data=sql.SQL(",").join(
                sql.Composed([sql.Identifier(k), sql.SQL("="), sql.Placeholder(k)]) for k in kwargs.keys() if
                k in data_keys)
        )

        self.execute(sql_query, kwargs)
        logger.debug(
            "Updating staging_epidemiology table with data: {}".format([kwargs[k] for k in kwargs.keys() if k not in data_keys]))
        
    def select_email(self, fsource: str):
        sql_query = sql.SQL("SELECT email from email_info WHERE fetcher_source = '%s'" %fsource) 
        # sql_query += "fetcher_source = '{}';".format( fsource )
        
        
        data=self.execute(sql_query)
        print(data)
        logger.debug(
            "selecting data from email info")
        return data
    
    def select_url(self):
        sql_query = sql.SQL("SELECT url, source_code, plugin_name from url_info")         
        data=self.execute(sql_query)
     
        logger.debug(
            "selecting data from email info")
        return data
    
    def update_url_status(self, status: str, source_code: str):
        
        sql_query = sql.SQL("update url_info set url_status_date = CURRENT_TIMESTAMP, url_status= '%s'  where source_code = '%s' RETURNING *" %(status, source_code))
        print(sql_query)
        
        self.execute(sql_query)
        logger.debug(
             "Uptading url status ")
        
    def call_db_function(self, source_code: str):
        compare_result= self.cur.callproc('covid19_compare_tables',[source_code])
        return compare_result
        
        
    def close_connection(self):
        if self.conn:
            if self.cur:
                self.cur.close_connection()
            self.conn.close_connection()
            logger.debug("Closing connection")
        self.conn = None
        self.cur = None
