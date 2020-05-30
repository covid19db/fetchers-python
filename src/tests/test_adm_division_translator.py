import unittest
import unittest.mock as mock
from io import StringIO

from utils.administrative_division_translator.translator import AdmTranslator

translation_csv = """\
input_adm_area_1,input_adm_area_2,input_adm_area_3,adm_area_1,adm_area_2,adm_area_3,gid
england,county durham,,England,County Durham,,GBR.1.30_1
england,darlington,,England,Darlington,,GBR.1.23_1"""


class AdmDivisionTranslatorTestCase(unittest.TestCase):

    def setUp(self):
        self.patcher = mock.patch('os.path.exists')
        mock_thing = self.patcher.start()
        mock_thing.side_effect = lambda x: True
        self.translator = AdmTranslator(csv_fname=StringIO(translation_csv))

    def tearDown(self):
        self.patcher.stop()

    def test_adm_division_translation(self):
        success, adm_area_1, adm_area_2, adm_area_3, gid = self.translator.tr(
            country_code='GBR',
            input_adm_area_1='england',
            input_adm_area_2='county durham',
            input_adm_area_3=None,
            return_original_if_failure=False,
            suppress_exception=False)

        self.assertTrue(success)
        self.assertTrue(adm_area_1 == 'England')
        self.assertTrue(adm_area_2 == 'County Durham')
        self.assertIsNone(adm_area_3)
        self.assertTrue(gid == ['GBR.1.30_1'])

    def test_adm_division_translation_return_original(self):
        success, adm_area_1, adm_area_2, adm_area_3, gid = self.translator.tr(
            country_code='GBR',
            input_adm_area_1='Test',
            input_adm_area_2='county durham',
            input_adm_area_3=None,
            return_original_if_failure=True,
            suppress_exception=True)

        self.assertFalse(success)
        self.assertTrue(adm_area_1 == 'Test')
        self.assertTrue(adm_area_2 == 'county durham')
        self.assertIsNone(adm_area_3)
        self.assertIsNone(gid)

    def test_adm_division_translation_unexisting_area(self):
        success, adm_area_1, adm_area_2, adm_area_3, gid = self.translator.tr(
            country_code='GBR',
            input_adm_area_1='Unknown',
            input_adm_area_2=None,
            input_adm_area_3=None,
            return_original_if_failure=False,
            suppress_exception=True)

        self.assertFalse(success)
        self.assertIsNone(adm_area_1)
        self.assertIsNone(adm_area_2)
        self.assertIsNone(adm_area_3)
        self.assertIsNone(gid)
