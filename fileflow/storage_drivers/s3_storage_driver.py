"""
.. module:: storage_drivers.s3_storage_driver
    :synopsis: S3 implementation of the base StorageDriver

.. moduleauthor:: David Barbarisi <dbarbarisi@industrydive.com>
"""

import boto

from .storage_driver import StorageDriver, StorageDriverError


class S3StorageDriver(StorageDriver):
    """
    Read and write to S3.
    """

    def __init__(self, access_key_id, secret_access_key, bucket_name):
        """
        Set up the credentials and bucket name.

        :param str access_key_id: AWS credentials.
        :param str secret_access_key: AWS credentials.
        :param str bucket_name: The S3 bucket to use.
        """
        super(S3StorageDriver, self).__init__()

        self.bucket_name = bucket_name

        self.s3 = boto.connect_s3(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key
        )
        self.bucket = self.s3.get_bucket(self.bucket_name)

    def get_filename(self, dag_id, task_id, execution_date):

        return 's3://{bucket_name}/{key_name}'.format(
            bucket_name=self.bucket_name,
            key_name=self.get_key_name(dag_id, task_id, execution_date)
        )

    def get_path(self, dag_id, task_id):
        return 's3://{bucket_name}/{dag_id}/{task_id}'.format(
            bucket_name=self.bucket_name,
            dag_id=dag_id,
            task_id=task_id
        )

    def get_key_name(self, dag_id, task_id, execution_date):
        """
        Formats the S3 key name for the given task instance.

        :param str dag_id: The airflow DAG ID.
        :param str task_id: The airflow task ID.
        :param datetime.datetime execution_date: The execution date of the task
            instance.
        :return: The S3 key name.
        :rtype: str
        """
        return '{dag_id}/{task_id}/{date}'.format(
            dag_id=dag_id,
            task_id=task_id,
            date=self.execution_date_string(execution_date)
        )

    def read(self, dag_id, task_id, execution_date, encoding='utf-8'):
        key_name = self.get_key_name(dag_id, task_id, execution_date)
        key = self.bucket.get_key(key_name)

        if key is not None:
            return key.get_contents_as_string(encoding=encoding)

        message = \
            'S3 key named {key_name} in bucket {bucket_name} does not exist.'.format(key_name=key_name,
                                                                                     bucket_name=self.bucket_name)

        raise StorageDriverError(message)

    def get_read_stream(self, dag_id, task_id, execution_date):
        key_name = self.get_key_name(dag_id, task_id, execution_date)
        key = self.bucket.get_key(key_name)

        if key is not None:
            import tempfile
            temp_file_stream = tempfile.TemporaryFile(mode='w+b')
            key.get_file(temp_file_stream)

            # Stream has been read in and is now at the end
            # So reset it to the start
            temp_file_stream.seek(0)

            return temp_file_stream

        message = \
            'S3 key named {key_name} in bucket {bucket_name} does not exist.'.format(key_name=key_name,
                                                                                     bucket_name=self.bucket_name)
        raise StorageDriverError(message)

    def write(self, dag_id, task_id, execution_date, data, content_type='text/plain', *args, **kwargs):
        """
        Note that content_type is an argument not in parent method.

        :param string content_type: The content-type. If set to None, it is not set.
        """
        key_name = self.get_key_name(dag_id, task_id, execution_date)

        key = self.get_or_create_key(key_name)

        if content_type is not None:
            key.set_metadata('Content-Type', content_type)

        key.set_contents_from_string(data)
        key.set_acl('private')

    def write_from_stream(self, dag_id, task_id, execution_date, stream, content_type='text/plain', *args, **kwargs):
        """
        :param string|None content_type: pass None to not set
        """
        # S3 does not like reading streams or chunks, so for now, just set it as a string and write it that way
        str = stream.read()
        self.write(dag_id, task_id, execution_date, str, content_type)

    def list_filenames_in_path(self, path):
        """
        This requires some special treatment. The path here is the full url
        path. For boto/s3 we need to strip out the s3 protocol and bucket
        name to only get the prefix.

        Everywhere else in life the path is in the url form. This is done to
        mimic the full path in the file system storage driver.
        """
        # Remove the s3 protocol and bucket name and initial slash to get at
        # the prefix s3 wants.
        prefix = path[len('s3://' + self.bucket_name + '/'):]

        # Make sure we have a trailing slash by removing any that might
        # already be there and then appending one.
        prefix = prefix.rstrip('/') + '/'

        results = self.bucket.list(prefix=prefix)

        # Get the key names from all found keys and cut off the path prefix.
        return [k.name[len(prefix):] for k in results]

    def get_or_create_key(self, key_name):
        """
        Get a boto Key object with the given key name. If the key exists,
        returns that. Otherwise creates a new key.

        :param str key_name: The name of the S3 key.
        :return: A boto key object.
        :rtype: boto.s3.key.Key
        """
        key = self.bucket.get_key(key_name)

        if key is None:
            key = self.bucket.new_key(key_name)

        return key
