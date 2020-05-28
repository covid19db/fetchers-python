import os
import time
import logging
import schedule

from utils.logger import setup_logger
from utils.plugins import Plugins
from utils.adapters import DataAdapter
from utils.adapter_wrapper import AdapterWrapper

logger = logging.getLogger(__name__)
VALIDATE_INPUT_DATA = os.environ.get("VALIDATE_INPUT_DATA", "").lower() == 'true'


def main():
    setup_logger()
    plugins = Plugins(validate_input_data=VALIDATE_INPUT_DATA)
    table_name_postfix = 'staging_' if VALIDATE_INPUT_DATA else None

    # get data adapter
    data_adapter = DataAdapter.get_adapter()
    db_wrapper = AdapterWrapper(data_adapter,
                                sliding_window_days=os.environ.get("SLIDING_WINDOW_DAYS"),
                                table_name_postfix=table_name_postfix)

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
