"""
.. module:: storage_drivers.file_storage_driver
    :synopsis: Local file implementation of the base StorageDriver

.. moduleauthor:: David Barbarisi <dbarbarisi@industrydive.com>
"""

import os
import codecs

from .storage_driver import StorageDriver


class FileStorageDriver(StorageDriver):
    """
    Read and write to the local file system.
    """

    def __init__(self, prefix):
        """
        Set up the base path for storage.

        :param str prefix: The prefix or base path to use.
        """
        super(FileStorageDriver, self).__init__()
        self.prefix = prefix

    def get_filename(self, dag_id, task_id, execution_date):
        return os.path.join(
            self.prefix,
            dag_id,
            task_id,
            self.execution_date_string(execution_date)
        )

    def get_path(self, dag_id, task_id):
        return os.path.join(self.prefix, dag_id, task_id)

    def read(self, dag_id, task_id, execution_date, encoding='utf-8'):
        filename = self.get_filename(dag_id, task_id, execution_date)

        with codecs.open(filename, 'rb', encoding=encoding) as f:
            data = f.read()

        return data

    def get_read_stream(self, dag_id, task_id, execution_date):
        filename = self.get_filename(dag_id, task_id, execution_date)

        f = codecs.open(filename, 'rb')
        return f

    def write(self, dag_id, task_id, execution_date, data, *args, **kwargs):
        # Note that content_type isn't used here.
        filename = self.get_filename(dag_id, task_id, execution_date)

        self.check_or_create_dir(os.path.dirname(filename))

        with open(filename, "w") as f:
            f.write(data)

    def write_from_stream(self, dag_id, task_id, execution_date, stream, *args, **kwargs):
        self.write(dag_id, task_id, execution_date, data=stream.read())

    def list_filenames_in_path(self, path):
        all_filenames = []
        for (dirpath, dirnames, filenames) in os.walk(path):
            all_filenames.extend(filenames)
            break

        return all_filenames

    def check_or_create_dir(self, dir):
        """
        Make sure our storage location exists.

        :param str dir: The directory name to look for and create if it
            doesn't exist..
        :return:
        """
        if not os.path.exists(dir):
            os.makedirs(dir)


