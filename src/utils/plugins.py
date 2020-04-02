import os
import sys
import logging
import inspect
import importlib
from typing import List
from pathlib import Path
from utils.fetcher_abstract import AbstractFetcher

logger = logging.getLogger(__name__)


def search_for_plugins() -> List:
    plugins_path = os.path.join(os.path.dirname(__file__), "../plugins")
    sys.path.append(plugins_path)
    available_plugins = []

    for path in Path(plugins_path).rglob('*.py'):
        module_path = path.relative_to(plugins_path).with_suffix('')
        module_name = str(module_path).replace('/', '.')
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

    return available_plugins
