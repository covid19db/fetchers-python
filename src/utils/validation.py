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

import logging
from utils.adapters import DataAdapter
from utils.email import send_email
from utils.fetcher_abstract import FetcherType

logger = logging.getLogger(__name__)


def validate_incoming_data(data_adapter: DataAdapter, fetcher_type: FetcherType, source_name: str):
    # TODO: Add fetcher type support, currently works only for epidemiology
    compare_result = data_adapter.call_db_function_compare(source_name)

    if compare_result[0] == 0:
        data_adapter.call_db_function_send_data(source_name)
        return True
    else:
        message = f"Validation failed for {source_name}, please check"
        try:
            send_email(source_name, message)
        except Exception as ex:
            logger.error(f'Unable to send an email {source_name}', exc_info=True)

        return False
