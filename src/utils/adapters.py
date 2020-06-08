from utils.config import config
from adapters.postgresql import PostgresqlHelper
from adapters.sqlite import SqliteHelper
from adapters.csvfile import CSVFileHelper
from utils.adapter_abstract import AbstractAdapter


class DataAdapter:

    @staticmethod
    def get_adapter() -> AbstractAdapter:
        if config.DB_USERNAME and config.DB_PASSWORD and config.DB_ADDRESS and config.DB_NAME:
            return PostgresqlHelper(user=config.DB_USERNAME,
                                    password=config.DB_PASSWORD,
                                    host=config.DB_ADDRESS,
                                    port=config.DB_PORT,
                                    database_name=config.DB_NAME)
        elif config.SQLITE:
            return SqliteHelper(sqlite_file_path=config.SQLITE)
        elif config.CSV:
            return CSVFileHelper(csv_path=config.CSV)
        else:
            raise ValueError('Unable to select serializer')
