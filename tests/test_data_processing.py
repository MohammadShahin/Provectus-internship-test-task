import unittest
from data_processing.data_processing import *


class TestHelpers(unittest.TestCase):
    def test_is_valid_headers_src(self):
        self.assertEqual(is_valid_headers_src(['first_name', ' last_name', ' birthts']), True)
        self.assertEqual(is_valid_headers_src(['first_name', 'last_name', 'birthts']), True)
        self.assertEqual(is_valid_headers_src(['first_name   ', '   last_name', '   birthts  ']), True)
        self.assertEqual(is_valid_headers_src(['first_name', ' last_name', ' birthts', 'sth']), False)
        self.assertEqual(is_valid_headers_src(['first_name', 'last_name', 'birthts', '']), False)
        self.assertEqual(is_valid_headers_src(['first_name', 'last_name', ' bir thts']), False)
        self.assertEqual(is_valid_headers_src(['first_name', ' ', 'last_name', ' bir thts']), False)

    def test_check_values_types_src(self):
        self.assertTupleEqual(check_values_types_src(['moh', ' salah ', ' 123455634']), (True, ''))
        self.assertTupleEqual(check_values_types_src(['a', ' w ', '23']), (True, ''))
        self.assertTupleEqual(check_values_types_src(['r', ' s ', ' 1  ']), (True, ''))
        self.assertTupleEqual(check_values_types_src(['yes qwe', 'no', ' 81923']), (True, ''))
        self.assertTupleEqual(check_values_types_src(['qwer', ' tyu ', ' 90']), (True, ''))
        self.assertTupleEqual(check_values_types_src(['qwer', ' tyu ', ' -90']), (True, ''))
        self.assertTupleEqual(check_values_types_src(['no', 'name', ' 123455634']), (True, ''))
        self.assertTupleEqual(check_values_types_src(['no', 'name', ' -123455634 ']), (True, ''))
        self.assertTupleEqual(check_values_types_src(['', 'name', '20']), (False, "The value  does not follow first_name's condition"))
        self.assertTupleEqual(check_values_types_src(['f', '', '20']), (False, "The value  does not follow last_name's condition"))
        self.assertTupleEqual(check_values_types_src(['f', 'y', '']), (False, "The value  does not follow birthts's condition"))
        self.assertTupleEqual(check_values_types_src(['f', 'y', '20 2']), (False, "The value 20 2 does not follow birthts's condition"))
        self.assertTupleEqual(check_values_types_src(['f', 'y', '20O']), (False, "The value 20O does not follow birthts's condition"))
        self.assertTupleEqual(check_values_types_src(['f', 'y', 'asd']), (False, "The value asd does not follow birthts's condition"))

