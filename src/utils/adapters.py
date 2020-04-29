import os
from adapters.postgresql import PostgresqlHelper
from adapters.sqlite import SqliteHelper


class DataAdapter:

    @staticmethod
    def get_adapter():
        if os.getenv('DB_USERNAME') and os.getenv('DB_PASSWORD') and os.getenv('DB_ADDRESS') and os.getenv('DB_NAME'):
            return PostgresqlHelper(user=os.getenv('DB_USERNAME'),
                                    password=os.getenv('DB_PASSWORD'),
                                    host=os.getenv('DB_ADDRESS'),
                                    port=int(os.getenv('DB_PORT', 5432)),
                                    database_name=os.getenv('DB_NAME'))
        elif os.getenv('SQLITE'):
            return SqliteHelper(sqlite_file_path=os.getenv('SQLITE'))
        else:
            raise ValueError('Unable to select serializer')
