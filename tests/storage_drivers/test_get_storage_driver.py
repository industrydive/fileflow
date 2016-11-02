from unittest import TestCase
from fileflow.storage_drivers import get_storage_driver, FileStorageDriver, S3StorageDriver
from fileflow.errors import FileflowError
from moto import mock_s3
from nose.plugins.attrib import attr
import boto


@attr('unittest')
@mock_s3
class TestGetStorageDriver(TestCase):
    def setUp(self):
        """
        Set up the moto connection.
        """
        self.conn = boto.connect_s3()

    def test_s3_production_environment(self):
        """
        Test the storage driver and bucket name when using S3 in production.
        """
        self.conn.create_bucket('the_bucket')

        # s3, bucket_name, production, any, any
        driver = get_storage_driver(
            's3', '', 'production', '', '', 'the_bucket'
        )
        self.assertIsInstance(driver, S3StorageDriver)
        self.assertEqual(driver.bucket_name, 'the_bucket')

    def test_s3_qa_environment(self):
        """
        Test the storage driver and bucket name when using S3 in QA.
        """
        self.conn.create_bucket('the_bucketqa')

        # s3, bucket_name, qa, key1, key2
        driver = get_storage_driver('s3', '', 'qa', '', '', 'the_bucket')
        self.assertIsInstance(driver, S3StorageDriver)
        self.assertEqual(driver.bucket_name, 'the_bucketqa')

    def test_s3_development_environment(self):
        """
        Test the storage driver and bucket name when using S3 in development.
        """
        self.conn.create_bucket('the_bucketdevelopment')

        # s3, bucket_name, development, key1, key2
        driver = get_storage_driver(
            's3', '', 'development', '', '', 'the_bucket'
        )
        self.assertIsInstance(driver, S3StorageDriver)
        self.assertEqual(driver.bucket_name, 'the_bucketdevelopment')

    def test_s3_test_environment(self):
        """
        Test the storage driver and bucket name when using S3 in test.
        """
        self.conn.create_bucket('the_buckettest')

        # s3, bucket_name, test, key1, key2
        driver = get_storage_driver('s3', '', 'test', '', '', 'the_bucket')
        self.assertIsInstance(driver, S3StorageDriver)
        self.assertEqual(driver.bucket_name, 'the_buckettest')

    def test_s3_bad_environment(self):
        """
        Test an error is raised when using an incorrect environment name for
        S3 storage.
        """
        # s3, bucket_name, bad_environment, key1, key2
        with self.assertRaises(FileflowError):
            get_storage_driver(
                's3', '', 'bad_environment', '', '', 'the_bucket'
            )

    def test_file_driver(self):
        """
        Test the file storage driver is returned when configured.
        """
        # file, prefix, anything, anything, anything
        driver = get_storage_driver('file', '/the/prefix/', '', '', '')
        self.assertIsInstance(driver, FileStorageDriver)
        self.assertEqual(driver.prefix, '/the/prefix/')

    def test_bad_storage_type(self):
        """
        Test an error is raised when an unknown storage type is configured.
        """
        # bad_storage_type, anything, anything, anything
        with self.assertRaises(FileflowError):
            get_storage_driver('bad_storage_type', '', '', '', '')
