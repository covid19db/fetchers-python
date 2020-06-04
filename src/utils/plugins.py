import os
import sys
import logging
import inspect
import importlib
from typing import List
from pathlib import Path

from utils.config import config
from utils.adapter_abstract import AbstractAdapter
from utils.fetcher_abstract import AbstractFetcher
from utils.validation import validate_incoming_data
from utils.decorators import timeit

logger = logging.getLogger(__name__)


class Plugins:

    def __init__(self):
        self.validate_input_data = config.VALIDATE_INPUT_DATA
        self.available_plugins = self.search_for_plugins()
        self.run_only_plugins = self.get_only_selected_plugins()

    @staticmethod
    def search_for_plugins() -> List:
        plugins_path = os.path.join(os.path.dirname(__file__), "..", "plugins")
        sys.path.append(plugins_path)
        available_plugins = []

        for path in Path(plugins_path).rglob('*.py'):
            module_path = path.relative_to(plugins_path).with_suffix('')
            module_name = str(module_path).replace('/', '.').replace('\\', '.')
            try:
                module = importlib.import_module(module_name)
                for item_name in dir(module):
                    obj = getattr(module, item_name)
                    if inspect.isclass(obj) and issubclass(obj, AbstractFetcher) and obj != AbstractFetcher:
                        if hasattr(obj, 'LOAD_PLUGIN') and obj.LOAD_PLUGIN:
                            logger.debug(f"Loading plugin: {obj.__name__}")
                            available_plugins.append(obj)
            except Exception as ex:
                logger.error(f'Unable to load plugin: {module_name}, error: {ex}')

        return sorted(available_plugins, key=lambda x: x.__name__)

    @staticmethod
    def get_only_selected_plugins() -> List:
        run_only_plugins = config.RUN_ONLY_PLUGINS
        if run_only_plugins:
            return run_only_plugins.split(",")
        return None

    @staticmethod
    def validate(plugin: AbstractFetcher, plugin_instance: AbstractFetcher,
                 data_adapter: AbstractAdapter) -> bool:
        fetcher_type = plugin_instance.TYPE if hasattr(plugin_instance, 'TYPE') else None

        if fetcher_type.value not in ['epidemiology']:
            # Exit without verification - fetcher type not supported yet
            return True

        source_name = plugin_instance.SOURCE if hasattr(plugin_instance, 'SOURCE') else None
        validation_success = validate_incoming_data(data_adapter, fetcher_type, source_name)
        if validation_success:
            logger.info(f"Validating source data for: {plugin.__name__}, source_name: {source_name} "
                        f"result: {validation_success}")
        else:
            logger.warning(f"Validating source data for: {plugin.__name__}, source_name: {source_name} "
                           f"result: {validation_success}")
        return validation_success

    @timeit
    def run_plugins_job(self, data_adapter: AbstractAdapter):
        for plugin in self.available_plugins:
            if self.run_only_plugins and plugin.__name__ not in self.run_only_plugins:
                continue
            self.run_single_plugin(data_adapter, plugin)

    @timeit
    def run_single_plugin(self, data_adapter: AbstractAdapter, plugin: AbstractFetcher):
        try:
            logger.info(f'Running plugin {plugin.__name__} ')
            if self.validate_input_data:
                data_adapter.truncate_staging()
            instance = plugin(db=data_adapter)
            instance.run()
            data_adapter.flush()
            validation_success = self.validate(plugin, instance, data_adapter) if self.validate_input_data else True

            if validation_success:
                logger.info(f"Plugin {plugin.__name__} finished successfully")
            else:
                logger.warning(f"Plugin {plugin.__name__} failed due to data discrepancy, email was sent")

        except Exception as ex:
            logger.error(f'Error running plugin {plugin.__name__}, exception: {ex}', exc_info=True)
