from utils.config import Config
from adapters.postgresql import PostgresqlHelper
from adapters.sqlite import SqliteHelper
from adapters.csvfile import CSVFileHelper
from utils.adapter_abstract import AbstractAdapter


class DataAdapter:

    @staticmethod
    def get_adapter() -> AbstractAdapter:
        if Config.DB_USERNAME and Config.DB_PASSWORD and Config.DB_ADDRESS and Config.DB_NAME:
            return PostgresqlHelper(user=Config.DB_USERNAME,
                                    password=Config.DB_PASSWORD,
                                    host=Config.DB_ADDRESS,
                                    port=Config.DB_PORT,
                                    database_name=Config.DB_NAME)
        elif Config.SQLITE:
            return SqliteHelper(sqlite_file_path=Config.SQLITE)
        elif Config.CSV:
            return CSVFileHelper(csv_path=Config.CSV)
        else:
            raise ValueError('Unable to select serializer')
