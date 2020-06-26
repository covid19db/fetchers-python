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

import os
import sys
import time
import logging
import inspect
import importlib
from typing import List
from pathlib import Path
from datetime import datetime

from utils.config import config
from utils.adapter.abstract_adapter import AbstractAdapter
from utils.email import send_email
from utils.fetcher.abstract_fetcher import AbstractFetcher
from utils.validation import validate_incoming_data
from utils.decorators import timeit
from utils.diagnostics import Diagnostics

logger = logging.getLogger(__name__)

__all__ = ('Plugins')


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

    def should_run_plugin(self, plugin_name: str) -> bool:
        if not self.run_only_plugins:
            return True

        for plugin in self.run_only_plugins:
            if plugin.startswith('-') and '-' + plugin_name == plugin:
                return False
            if plugin_name == plugin or plugin == '*':
                return True

        return False

    @staticmethod
    def validate_consistency(plugin: AbstractFetcher, plugin_instance: AbstractFetcher,
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
            logger.warning(f"Plugin {plugin.__name__} failed due to data discrepancy, email was sent")

        return validation_success

    @staticmethod
    def validate_latest_timestamp(plugin: AbstractFetcher, plugin_instance: AbstractFetcher):
        if not config.VALIDATE_LATEST_TS_DAYS:
            return

        latest_timestamp = plugin_instance.get_latest_timestamp()
        if latest_timestamp:
            days = (datetime.now().date() - latest_timestamp).days
            if days > config.VALIDATE_LATEST_TS_DAYS:
                message = f"Plugin {plugin.__name__} has latest_timestamp {days} days old, " \
                          f"max is {config.VALIDATE_LATEST_TS_DAYS}"
                logger.info(message)
                subject = f"Covid19db fetchers-python latest_timestamp validation error"
                try:
                    send_email(plugin_instance.SOURCE, subject, message)
                except Exception as ex:
                    logger.error(f'Unable to send an email {plugin.__name__}', exc_info=True)

    @timeit
    def run_plugins_job(self, data_adapter: AbstractAdapter):
        for plugin in self.available_plugins:
            if self.should_run_plugin(plugin.__name__):
                self.run_single_plugin(data_adapter, plugin)

    @timeit
    def run_single_plugin(self, data_adapter: AbstractAdapter, plugin: AbstractFetcher):
        logger.info(f'Running plugin {plugin.__name__} ')
        error = False
        validation_success = None
        start_time = time.time()
        try:
            if self.validate_input_data:
                data_adapter.truncate_staging()
            plugin_instance = plugin(data_adapter)
            plugin_instance.run()
            data_adapter.flush()
            validation_success = self.validate_consistency(plugin,
                                                           plugin_instance,
                                                           data_adapter) if self.validate_input_data else True
            self.validate_latest_timestamp(plugin, plugin_instance)

            if validation_success:
                logger.info(f"Plugin {plugin.__name__} finished successfully")

        except Exception as ex:
            error = True
            logger.error(f'Error running plugin {plugin.__name__}, exception: {ex}', exc_info=True)

        Diagnostics(plugin_instance).update_diagnostics_info(
            validation=validation_success,
            error=error,
            start_time=start_time,
            end_time=time.time()
        )
