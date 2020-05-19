import time
import logging
import schedule
from typing import List

from utils.logger import setup_logger
from utils.plugins import search_for_plugins, get_only_selected_plugins
from utils.adapters import DataAdapter

from utils.adapter_abstract import AbstractAdapter

logger = logging.getLogger(__name__)


def run_plugins_job(db: AbstractAdapter, available_plugins: List, run_only_plugins: List = None):
    for plugin in available_plugins:
        if run_only_plugins and plugin.__name__ not in run_only_plugins:
            continue
        try:
            logger.info(f'Running plugin {plugin.__name__} ')
            instance = plugin(db=db)
            instance.run()
            db.flush()
            logger.info(f"Plugin {plugin.__name__} finished successfully")
        except Exception as ex:
            logger.error(f'Error running plugin {plugin.__name__}, exception: {ex}', exc_info=True)


def main():
    setup_logger()
    db = DataAdapter.get_adapter()
    available_plugins = search_for_plugins()

    # run once
    run_plugins_job(db=db, available_plugins=available_plugins,
                    run_only_plugins=get_only_selected_plugins())
    # run every day at 2am
    schedule.every().day.at("02:00").do(run_plugins_job, db=db, available_plugins=available_plugins)

    logger.debug('Run schedule job every day at 02:00')
    while True:
        schedule.run_pending()
        time.sleep(10)


if __name__ == "__main__":
    main()
