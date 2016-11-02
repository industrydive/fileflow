"""
.. moduleauthor:: Donald Vetal <dvetal@industrydive.com>
.. moduleauthor:: Laura Lorenz <llorenz@industrydive.com>
.. moduleauthor:: Miriam Sexton <miriam@industrydive.com>
"""


from unittest import TestCase
from fileflow.utils import read_and_clean_csv_to_dataframe, clean_and_write_dataframe_to_csv
import numpy as np
import pandas as pd
import codecs
import os
import os.path
from moto import mock_s3
import boto
from nose.plugins.attrib import attr


@attr('unittest')
class TestPandasCsv(TestCase):
    def setUp(self):
        self.input_filename = "tests/fixtures/utils/example-dataframe.csv"
        self.output_filename = "tests/test-output/dataframe-csv"

        if not os.path.exists("tests/test-output"):
            os.makedirs("tests/test-output")

    @mock_s3
    def test_read_and_clean_csv_to_dataframe_with_s3(self):
        """
        Test reading a csv from S3 directly with pandas.
        Test reading a csv from S3 directly with pandas.
        """

        # Load the fixture into mock S3.
        with open(self.input_filename, 'r') as f:
            expected = f.read()

        conn = boto.connect_s3()
        conn.create_bucket('pandas_test_bucket')
        bucket = conn.get_bucket('pandas_test_bucket')
        key = bucket.new_key('test/pandas/fixture.csv')
        key.set_contents_from_string(expected)

        # Now try to read and clean it.
        s3_url = 's3://pandas_test_bucket/test/pandas/fixture.csv'

        actual = read_and_clean_csv_to_dataframe(s3_url)

        self.assertIsInstance(actual, pd.DataFrame)

    @mock_s3
    def test_read_and_clean_csv_to_dataframe_with_s3_write_stream(self):
        from fileflow.storage_drivers import S3StorageDriver
        import datetime
        import csv

        input_filename = 'tests/fixtures/utils/test_malformed_csv_ingest_tiny.csv'
        conn = boto.connect_s3()
        conn.create_bucket('pandas_test_bucket')

        dag_id = 'test_dag'
        task_id = 'test_read_csv_task'
        date = datetime.datetime(2016, 1, 15)
        storage = S3StorageDriver('ACCESS_KEY', 'SECRECT', bucket_name='pandas_test_bucket')

        with open(input_filename, 'r') as f:
            storage.write_from_stream(dag_id, task_id, date, f)

        s3_url = ''.join([
            's3://',
            storage.bucket_name,
            '/',
            storage.get_key_name(dag_id, task_id, date)
        ])

        # Read in locally and compare
        with open(input_filename, 'r') as f:
            reader = csv.reader(f)

            header_row = reader.next()
            all_rows = [r for r in reader]
        expected = pd.DataFrame(all_rows, columns=header_row)

        actual = read_and_clean_csv_to_dataframe(s3_url)

        self.assertIsInstance(actual, pd.DataFrame)
        self.assertEqual(actual.shape, expected.shape)
        self.assertListEqual(actual.to_dict(orient='records'), expected.to_dict(orient='records'))

        # Read in from 's3' and compare
        inp = storage.get_read_stream(dag_id, task_id, date)
        reader = csv.reader(inp)
        header_row = reader.next()
        all_rows = [r for r in reader]
        read_in = pd.DataFrame(all_rows, columns=header_row)

        self.assertEqual(actual.shape, read_in.shape)
        self.assertListEqual(actual.to_dict(orient='records'), read_in.to_dict(orient='records'))

    def test_read_and_write(self):
        """
        Test that a csv can be read into pandas and then back out to a csv.
        :return:
        """
        # test data can be pulled from file csv.
        input_data = read_and_clean_csv_to_dataframe(self.input_filename)

        # test the file can be written
        clean_and_write_dataframe_to_csv(input_data, self.output_filename)

        data_from_output = read_and_clean_csv_to_dataframe(self.output_filename)

        self.assertEqual(input_data.shape, data_from_output.shape)

        # assert their values are the same
        for row_number in xrange(input_data.shape[0]):
            for column_number in xrange(input_data.shape[1]):
                self.assertEqual(input_data.iloc[row_number, column_number],
                                 data_from_output.iloc[row_number, column_number])

    def test_read_convert_to_none(self):
        """
        Test that read_and_clean_csv_to_dataframe method handles nulls as expected

        :return:
        """
        # pull in input data
        input_data = read_and_clean_csv_to_dataframe(self.input_filename)
        self.assertIsNone(input_data.iloc[7, 3])
        self.assertIsNone(input_data.iloc[8, 4])
        self.assertIsNone(input_data.iloc[9, 2])

    def test_read_convert_utf8(self):
        """
        Test that read_and_clean_csv_to_dataframe method can properly decode utf-8 characters

        :return:
        """
        # pull in input data
        input_data = read_and_clean_csv_to_dataframe(self.input_filename)
        # grab our unicode character
        utf_8_cell_value = input_data.iloc[6, 0]
        # confirm we are dealing with unicode type, not str type
        self.assertEqual(type(utf_8_cell_value), unicode)
        # confirm it is the right unicode character
        self.assertEqual(utf_8_cell_value[-1:], u"\ufffd")

    def test_read_and_clean_csv_with_empty_content(self):
        """
        Test that read_and_clean_csv_to_dataframe can deal with a dataframe-like CSV with no content

        """
        expected_dataframe = pd.DataFrame(columns=['column1', 'column2'])

        actual_dataframe = read_and_clean_csv_to_dataframe("tests/fixtures/utils/empty_dataframelike_csv.csv")

        # columns the same
        self.assertItemsEqual(actual_dataframe.columns, expected_dataframe.columns)
        # content the same
        self.assertItemsEqual(actual_dataframe.to_dict(), expected_dataframe.to_dict())

        # no errors! yay! that means we got around bug from https://github.com/pydata/pandas/issues/12048

    def test_write_convert_to_none(self):
        """
        Test that clean_and_write_dataframe_to_csv method handles np.NaN's as expected
        :return:
        """

        # create a dataframe with np.NaN's in it
        df = pd.DataFrame([np.nan])
        clean_and_write_dataframe_to_csv(df, self.output_filename)
        with codecs.open(self.output_filename, "r", encoding="utf-8") as f:
            data = f.readlines()
        # we expect a quoted csv field with a \n line terminator at the end
        self.assertEqual(data[0], '"0"\n')
        self.assertEqual(data[1], '"None"\n')

    def test_write_utf8(self):
        """
        Test that clean_and_write_dataframe_to_csv method preserves utf8 encoding
        :return:
        """

        # create a dataframe with a utf8 character in it
        df = pd.DataFrame([u"\ufffd"])
        clean_and_write_dataframe_to_csv(df, self.output_filename)
        with codecs.open(self.output_filename, "r", encoding="utf-8") as f:
            data = f.readlines()
        # we expect a quoted csv field with a \n line terminator at the end
        self.assertEqual(data[1], u'"\ufffd"\n')

    def tearDown(self):
        try:
            os.remove(self.output_filename)
        except OSError:
            # file was not written in this test
            pass
