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
import logging
import schedule

from utils.logger import setup_logger
from utils.plugins import Plugins
from utils.adapters import DataAdapter
from utils.adapter_wrapper import AdapterWrapper

logger = logging.getLogger(__name__)


def main():
    setup_logger()
    plugins = Plugins()

    # get data adapter
    data_adapter = DataAdapter.get_adapter()
    db_wrapper = AdapterWrapper(data_adapter)

    # run once
    plugins.run_plugins_job(data_adapter=db_wrapper)
    # run every day at 2am
    schedule.every().day.at("02:00").do(plugins.run_plugins_job, data_adapter=db_wrapper)

    logger.debug('Run schedule job every day at 02:00')
    while True:
        schedule.run_pending()
        time.sleep(10)


if __name__ == "__main__":
    main()
