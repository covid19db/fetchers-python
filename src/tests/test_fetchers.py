import unittest

from utils.plugins import Plugins


class FetchersTestCase(unittest.TestCase):

    def setUp(self):
        plugins = Plugins()
        self.available_plugins = plugins.available_plugins

    def test_attribute_load_plugin(self):
        for plugin in self.available_plugins:
            self.assertTrue(hasattr(plugin, 'LOAD_PLUGIN'),
                            f"Plugin '{plugin.__name__}' has no 'LOAD_PLUGIN' attribute")

    def test_attribute_source(self):
        for plugin in self.available_plugins:
            self.assertTrue(hasattr(plugin, 'SOURCE'),
                            f"Plugin '{plugin.__name__}' has no 'SOURCE' attribute")

    def test_check_run_method(self):
        for plugin in self.available_plugins:
            run_method = getattr(plugin, 'run', None)
            self.assertTrue(run_method and callable(run_method),
                            f"Plugin '{plugin.__name__}' has no 'run' method")
