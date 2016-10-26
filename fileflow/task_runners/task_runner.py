"""
.. module:: task_runners.task_runner
    :synopsis: Base TaskRunner class to use with DivePythonOperator

.. moduleauthor:: Laura Lorenz <llorenz@industrydive.com>
.. moduleauthor:: Miriam Sexton <miriam@industrydive.com>
"""

import datetime
import json

from fileflow.utils import read_and_clean_csv_to_dataframe, clean_and_write_dataframe_to_csv
from fileflow.storage_drivers import get_storage_driver

class TaskRunner(object):
    def __init__(self, context):

        # The upstream dependencies
        # These must always be specified
        # Dictionary can contain any number of keys which must be redirected in the business logic to their read/parse methods
        self.data_dependencies = context.pop('data_dependencies', {})

        # The task instance.
        self.task_instance = context['ti']
        self.date = context['execution_date']

        # Picking a storage driver for this task instance.
        self.storage = get_storage_driver()

    def get_input_filename(self, data_dependency, dag_id=None):
        """
        Generate the default input filename for a class.

        :param str data_dependency: Key for the target data_dependency in
            self.data_dependencies that you want to construct a filename for.
        :param str dag_id: Defaults to the current DAG id
        :return: File system path or S3 URL to the input file.
        :rtype: str
        """
        if dag_id is None:
            dag_id = self.task_instance.dag_id

        task_id = self.data_dependencies[data_dependency]

        return self.storage.get_filename(dag_id, task_id, self.date)

    def get_output_filename(self):
        """
        Generate the default output filename or S3 URL for this task instance.

        :return: File system path to output filename
        :rtype: str
        """
        return self.storage.get_filename(
            self.task_instance.dag_id,
            self.task_instance.task_id,
            self.date
        )

    def get_upstream_stream(self, data_dependency_key, dag_id=None):
        """
        Returns a stream to the file that was output by a seperate task in the same dag.

        :param str data_dependency_key: The key (business logic name) for the
            upstream dependency. This will get the value from the
            self.data_dependencies dictionary to determine the file to read
            from.
        :param str dag_id: Defaults to the current DAG id.
        :param str encoding: The file encoding to use. Defaults to 'utf-8'.
        :return: stream to the file
        :rtype: stream
        """
        if dag_id is None:
            dag_id = self.task_instance.dag_id

        task_id = self.data_dependencies[data_dependency_key]
        stream = self.storage.get_read_stream(dag_id, task_id, self.date)

        # Just make 100% sure we're at the beginning
        stream.seek(0)

        return stream

    def read_upstream_file(self, data_dependency_key, dag_id=None, encoding='utf-8'):
        """
        Reads the file that was output by a seperate task in the same dag.

        :param str data_dependency_key: The key (business logic name) for the
            upstream dependency. This will get the value from the
            self.data_dependencies dictionary to determine the file to read
            from.
        :param str dag_id: Defaults to the current DAG id.
        :param str encoding: The file encoding to use. Defaults to 'utf-8'.
        :return: Result of reading the file
        :rtype: str
        """
        if dag_id is None:
            dag_id = self.task_instance.dag_id

        task_id = self.data_dependencies[data_dependency_key]

        return self.storage.read(dag_id, task_id, self.date, encoding=encoding)

    def read_upstream_pandas_csv(self, data_dependency_key, dag_id=None, encoding='utf-8'):
        """
        Reads a csv file from upstream into a pandas DataFrame. Specifically
        reads a csv into memory as a pandas dataframe in a standard
        manner. Reads the data in from a file output by a previous task.

        :param str data_dependency_key: The key (business logic name) for the
            upstream dependency. This will get the value from the
            self.data_dependencies dictionary to determine the file to read
            from.
        :param str dag_id: Defaults to the current DAG id.
        :param str encoding: The file encoding to use. Defaults to 'utf-8'.
        :return: The pandas dataframe.
        :rtype: :py:obj:`pd.DataFrame`
        """
        # Read the upstream file as a stream, abstracting away storage concerns
        input_stream = self.get_upstream_stream(data_dependency_key, dag_id)

        return read_and_clean_csv_to_dataframe(
            filename_or_stream=input_stream,
            encoding=encoding
        )

    def read_upstream_json(self, data_dependency_key, dag_id=None, encoding='utf-8'):
        """
        Reads a json file from upstream into a python object.

        :param str data_dependency_key: The key for the upstream data
            dependency. This will get the value from the
            self.data_dependencies dict to determine the file to read.
        :param str dag_id: Defaults to the current DAG id.
        :param str encoding: The file encoding. Defaults to 'utf-8'.
        :return: A python object.
        """
        return json.loads(
            self.read_upstream_file(
                data_dependency_key,
                dag_id,
                encoding=encoding
            )
        )

    def write_file(self, data, content_type='text/plain'):
        """
        Writes the data out to the correct file.

        :param str data: The data to output.
        :param str content_type: The Content-Type to use. Currently only used
            by S3.
        """
        self.storage.write(
            self.task_instance.dag_id,
            self.task_instance.task_id,
            self.date,
            data,
            content_type=content_type
        )

    def write_from_stream(self, stream, content_type='text/plain'):
        self.storage.write_from_stream(
            self.task_instance.dag_id,
            self.task_instance.task_id,
            self.date,
            stream,
            content_type=content_type
        )

    def write_timestamp_file(self):
        """
        Writes an output file with the current timestamp.
        """
        json = {'RUN': datetime.datetime.now().isoformat()}

        self.write_json(json)

    def write_pandas_csv(self, data):
        """
        Specifically writes a csv from a pandas dataframe to the default
        output file in a standard manner.

        :param data: the dataframe to write.
        """
        # When you pass filename=None, the result is returned as a string
        output = clean_and_write_dataframe_to_csv(data=data, filename=None)

        self.write_file(output, content_type='text/csv')

    def write_json(self, data):
        """
        Write a python object to a JSON output file.

        :param object data: The python object to save.
        """
        # TODO: Kinda weird that we embed the json.dumps() as we do since
        # it doesn't match the other conveience methods. Consider separating
        self.write_file(json.dumps(data), content_type='application/json')

    def run(self, *args, **kwargs):
        raise NotImplementedError("You must implement the run method for this task class.")

