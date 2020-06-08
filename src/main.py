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
