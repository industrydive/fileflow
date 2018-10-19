from unittest import TestCase
from fileflow.storage_drivers import FileStorageDriver
from datetime import datetime
from mock import MagicMock
from nose.plugins.attrib import attr
import os


@attr('unittest')
class TestFileStorageDriver(TestCase):
    def test_get_filename(self):
        """
        Test that get_filename returns the expected value.
        """
        expected = '/this/is/a/test/the_dag/the_task/1983-09-05'

        driver = FileStorageDriver('/this/is/a/test')
        actual = driver.get_filename(
            'the_dag',
            'the_task',
            datetime(1983, 9, 5)
        )

        self.assertEqual(actual, expected)

    def test_get_path(self):
        """
        Test that get_path returns the expected path value.
        """

        expected = '/this/is/a/test/the_dag/the_task'

        driver = FileStorageDriver('/this/is/a/test')
        actual = driver.get_path('the_dag', 'the_task')

        self.assertEqual(actual, expected)

    def test_read(self):
        """
        Test that we can read a fixture file on the local file system using an
        encoding..
        """
        filepath = 'tests/fixtures/FileStorageDriverReadTest.txt'
        expected = 'this is a reading test.'
        driver = FileStorageDriver('')

        driver.get_filename = MagicMock(return_value=filepath)

        actual = driver.read('the_dag', 'the_task', datetime(1983, 9, 5), 'utf-8')

        self.assertEqual(actual, expected)

    def test_get_read_stream_simple(self):
        """
        Test reading a stream from the file system
        """
        fake_read_stream_args = ['1', '2', datetime(2016, 1, 1)]

        driver = FileStorageDriver('.')
        driver.get_filename = MagicMock(return_value='tests/fixtures/SampleUniformData.json')
        string_value = open('tests/fixtures/SampleUniformData.json', 'r').read()
        actual_result = driver.get_read_stream(*fake_read_stream_args)

        self.assertEqual(actual_result.read(), string_value)

    def test_write(self):
        """
        Test that we can write to the local file system.
        :return:
        """
        driver = FileStorageDriver('')
        filepath = 'tests/test-output/FileStorageDriverWriteTest.txt'

        # Make sure it doesn't exist already
        if os.path.exists(filepath):
            os.remove(filepath)

        driver.get_filename = MagicMock(return_value=filepath)

        data = 'this is the data to write.'

        driver.write('the_dag', 'the_task', datetime(1983, 9, 5), data)

        self.assertTrue(os.path.exists(filepath))

        # Make sure we wrote the correct thing
        with open(filepath, 'r') as f:
            read_data = f.readline()
            self.assertEqual(read_data, data)

        read_data = open(filepath, 'r').readline()

        # Clean up.
        os.remove(filepath)

    def test_write_from_stream(self):
        """
        Test writing to the file system from a stream
        """
        driver = FileStorageDriver('.')
        filepath = 'tests/test-output/test_write_from_stream.txt'

        # Make sure it doesn't exist already
        if os.path.exists(filepath):
            os.remove(filepath)

        driver.get_filename = MagicMock(return_value=filepath)

        f = open('tests/fixtures/SampleUniformData.json', 'r')
        driver.write_from_stream('the_dag', 'the_task', datetime(1986, 4, 30), f)

        # Go back to the start so we can read it in
        f.seek(0)
        file_data = f.read()

        actual_data = open(filepath, 'r').read()
        self.assertEqual(actual_data, file_data)

    def test_list_filenames_in_path(self):
        """
        Test listing the filenames in a directory.
        """
        driver = FileStorageDriver('')

        fixture_path = os.path.join('tests', 'fixtures', 'example_dates')
        filenames = driver.list_filenames_in_path(fixture_path)

        expected = [
            '2016-01-01',
            '2016-01-02',
            '2016-01-03'
        ]

        self.assertItemsEqual(filenames, expected)

    def test_check_or_create_dir(self):
        """
        Test creating a directory if it doesn't exist, and doing nothing if
        it does.
        """
        dir_name = 'tests/test-output/this-dir-does-not-exist'

        # Confirm our starting state.
        self.assertFalse(os.path.exists(dir_name))

        driver = FileStorageDriver('')
        driver.check_or_create_dir(dir_name)

        # On the first pass we're creating the directory.
        self.assertTrue(os.path.exists(dir_name))

        # On the second pass we should do nothing.
        driver.check_or_create_dir(dir_name)

        self.assertTrue(os.path.exists(dir_name))

        # Clean up.
        os.rmdir(dir_name)
