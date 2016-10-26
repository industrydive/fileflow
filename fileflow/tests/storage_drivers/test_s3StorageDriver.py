from unittest import TestCase
from datadive.storagedrivers import S3StorageDriver
from datetime import datetime
from mock import MagicMock
from moto import mock_s3
from nose.plugins.attrib import attr
import boto


@attr('unittest')
@mock_s3
class TestS3StorageDriver(TestCase):
    def setUp(self):
        """
        Set up a mock S3 connection, bucket, and key, using moto.
        """
        self.bucket_name = 's3storagesdrivertest'
        conn = boto.connect_s3()
        # We need to create the bucket since this is all in Moto's 'virtual' AWS account
        conn.create_bucket(self.bucket_name)

        self.bucket = conn.get_bucket(self.bucket_name)
        key = self.bucket.new_key('the_dag/the_task/1983-09-05')

        data = 'this is a test.'
        key.set_metadata('Content-Type', 'text/plain')
        key.set_contents_from_string(data)
        key.set_acl('private')

        self.driver = S3StorageDriver('', '', self.bucket_name)

    def test_get_filename(self):
        """
        Test that we are correctly constructing a full S3 URL from task
        instance data.
        """
        self.driver.get_key_name = MagicMock(return_value='the_key_name')

        expected = 's3://' + self.bucket_name + '/the_key_name'

        actual = self.driver.get_filename('the_dag', 'the_task', datetime(1983, 9, 5))

        self.assertEqual(actual, expected)

    def test_get_path(self):
        """
        Test getting the correct S3 path.
        """
        expected = 's3://' + self.bucket_name + '/the_dag/the_task'

        actual = self.driver.get_path('the_dag', 'the_task')

        self.assertEqual(actual, expected)

    def test_get_key_name(self):
        """
        Test that we are correctly constructing an S3 key name using the task
        instance data.
        """
        expected = 'the_dag/the_task/1983-09-05'

        actual = self.driver.get_key_name('the_dag', 'the_task', datetime(1983, 9, 5))

        self.assertEqual(actual, expected)

    def test_read(self):
        """
        Test reading from S3 via boto.
        """
        expected = 'this is a test.'
        actual = self.driver.read('the_dag', 'the_task', datetime(1983, 9, 5), 'utf-8')

        self.assertEqual(actual, expected)

    def test_get_read_stream(self):
        """
        Test reading a stream from S3
        """
        import tempfile

        first_value = 'abc'
        second_value_file = open('fixtures/subscriber/test_job_levels.json', 'r')
        second_value_string = second_value_file.read()
        second_value_file.seek(0)
        third_value_bytes = bytearray(['a', 'b', '\xe4'])
        third_value_file = tempfile.TemporaryFile()
        third_value_file.write(third_value_bytes)
        third_value_file.seek(0)

        dag_name = 'the_dag'
        task_name = 'the_task'

        first_key = self.bucket.new_key(dag_name + '/' + task_name + '/' + '2016-01-01')
        first_key.set_contents_from_string(first_value)
        first_key.set_metadata('Content-Type', 'text/plain')
        first_key.set_contents_from_string(first_value)
        first_key.set_acl('private')

        first_result_stream = self.driver.get_read_stream(
            dag_id=dag_name,
            task_id=task_name,
            execution_date=datetime(2016, 1, 1)
        )
        self.assertEqual(first_value, first_result_stream.read())

        second_key = self.bucket.new_key(dag_name + '/' + task_name + '/' + '2016-01-02')
        second_key.set_metadata('Content-Type', 'text/plain')
        second_key.set_contents_from_file(second_value_file)
        second_key.set_acl('private')

        second_result_stream = self.driver.get_read_stream(
            dag_id=dag_name,
            task_id=task_name,
            execution_date=datetime(2016, 1, 2)
        )
        self.assertEqual(second_value_string, second_result_stream.read())

        third_key = self.bucket.new_key(dag_name + '/' + task_name + '/' + '2016-01-03')
        third_key.set_contents_from_file(third_value_file)
        third_key.set_acl('private')

        third_result_stream = self.driver.get_read_stream(
            dag_id=dag_name,
            task_id=task_name,
            execution_date=datetime(2016, 1, 3)
        )
        self.assertEqual(third_value_bytes, third_result_stream.read())

    def test_write(self):
        """
        Test writing to S3 via boto.
        """
        data = 'this is a test write.'
        key_name = 'the_dag/the_task/1983-09-05'
        self.driver.write('the_dag', 'the_task', datetime(1983, 9, 5), data, 'text/plain')

        s3_key = self.bucket.get_key(key_name)
        actual = s3_key.get_contents_as_string()

        self.assertEqual(actual, data)

    def test_write_from_stream(self):
        """
        Test writing to S3 from a stream
        """
        import tempfile
        stream = tempfile.TemporaryFile()

        some_string = 'abc'
        stream.write(some_string)
        stream.seek(0)

        first_key_name = 'the_dag/the_task/1986-04-29'

        self.driver.write_from_stream('the_dag', 'the_task', datetime(1986, 4, 29), stream)
        stream.close()
        s3_key = self.bucket.get_key(first_key_name)
        actual_data = s3_key.get_contents_as_string()
        self.assertEqual(some_string, actual_data)

        # Try longer data from a file
        second_key_name = 'the_dag/the_task/1986-04-30'
        f = open('fixtures/subscriber/test_job_levels.json', 'r')
        self.driver.write_from_stream('the_dag', 'the_task', datetime(1986, 4, 30), f)

        # Go back to the start so we can read it in
        f.seek(0)
        file_data = f.read()

        s3_key = self.bucket.get_key(second_key_name)
        actual_data = s3_key.get_contents_as_string()

        self.assertEqual(file_data, actual_data)

        # Try something that's not a simple string
        some_values = bytearray(['a', 'b', '\xe4'])
        stream = tempfile.TemporaryFile()
        stream.write(some_values)
        stream.seek(0)

        third_key_name = 'the_dag/the_task/1986-04-01'
        self.driver.write_from_stream('the_dag', 'the_task', datetime(1986, 4, 1), stream, content_type=None)
        stream.close()
        s3_key = self.bucket.get_key(third_key_name)

        # Check that the default content type was used
        self.assertEqual(s3_key.content_type, 'application/octet-stream')

        actual_data_as_string = s3_key.get_contents_as_string()
        self.assertIsInstance(actual_data_as_string, str)
        self.assertEqual(str(some_values), actual_data_as_string)

        output_stream = tempfile.TemporaryFile()
        s3_key.get_file(output_stream)
        output_stream.seek(0)
        self.assertEqual(str(some_values), output_stream.read())

    def test_list_filenames_in_path(self):
        """
        Test listing the keys in a prefix.
        """
        prefix = 'fixtures/timeseries_demographic_data/biopharmadive_demographic_overall_daily'

        # The list_filenames_in_path method expects a full path.
        path = 's3://{bucket}/{prefix}'.format(
            bucket=self.bucket_name,
            prefix=prefix
        )

        # Create three keys.
        for index in range(1, 4):

            key = self.bucket.new_key('{}/2016-01-0{}'.format(prefix, index))

            data = 'this is a test.'
            key.set_metadata('Content-Type', 'text/plain')
            key.set_contents_from_string(data)
            key.set_acl('private')

        filenames = self.driver.list_filenames_in_path(path)

        expected = [
            '2016-01-01',
            '2016-01-02',
            '2016-01-03'
        ]

        self.assertListEqual(filenames, expected)

    def test_get_or_create_key(self):
        """
        Test that we can create and retrieve S3 key data.
        """
        key_name = 'the_dag/the_task/2015-12-01'
        expected = 'this is the created file.'

        # Make sure it didn't exist before
        none_existing_key = self.driver.s3.get_bucket(self.bucket_name).get_key(key_name)
        self.assertIsNone(none_existing_key)

        # Does the create.
        key = self.driver.get_or_create_key(key_name)
        self.assertIsNotNone(key)

        key.set_contents_from_string(expected)

        # Does the get.
        existing_key = self.driver.get_or_create_key(key_name)
        content = existing_key.get_contents_as_string()

        self.assertEqual(content, expected)
