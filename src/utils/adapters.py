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
