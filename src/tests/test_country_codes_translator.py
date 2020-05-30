import unittest

from utils.country_codes_translator.translator import CountryCodesTranslator


class CountryCodesTranslatorTestCase(unittest.TestCase):

    def setUp(self):
        self.translator = CountryCodesTranslator()

    def test_translation_country_a2_code(self):
        country, countrycode = self.translator.get_country_info(country_a2_code='GB')
        self.assertTrue(country == 'United Kingdom')
        self.assertTrue(countrycode == 'GBR')

    def test_translation_country_name(self):
        country, countrycode = self.translator.get_country_info(country_name='United Kingdom')
        self.assertTrue(country == 'United Kingdom')
        self.assertTrue(countrycode == 'GBR')

    def test_translation_unexisting_country_name(self):
        country, countrycode = self.translator.get_country_info(country_name='Unknown')
        self.assertIsNone(country)
        self.assertIsNone(countrycode)

    def test_translation_empty(self):
        country, countrycode = self.translator.get_country_info()
        self.assertIsNone(country)
        self.assertIsNone(countrycode)
