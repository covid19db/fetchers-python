import unittest
from inspect import signature

from utils.plugins import Plugins


class FetchersTestCase(unittest.TestCase):

    def setUp(self):
        plugins = Plugins()
        self.available_plugins = plugins.available_plugins
        self.run_only_plugins = plugins.run_only_plugins

    def test_check_run_method(self):
        for plugin in self.available_plugins:
            if self.run_only_plugins and plugin.__name__ not in self.run_only_plugins:
                continue

            run_method = getattr(plugin, 'fetch', None)
            if callable(run_method):
                print(f"Fetching data for '{plugin.__name__}'....")
                instance = plugin(db=None)

                fetch_method = instance.fetch
                sign = signature(fetch_method)
                if len(sign.parameters) == 0:
                    result = instance.fetch()

                    self.assertIsNotNone(result, f"Plugin '{plugin.__name__}' fetch() returns None")
                    print(f"Success")
                else:
                    print(f'Unable to check {plugin.__name__}, signature: fetch{sign}')
