"""
.. module:: storage_drivers.storage_driver
    :synopsis: Abstraction of the file system for reading and writing task files.

.. moduleauthor:: David Barbarisi <dbarbarisi@industrydive.com>
"""


class StorageDriver(object):
    """
    A base class for common functionality and API amongst the storage drivers.

    This is an example of mocking the read method inside a :py:class:`~fileflow.operators.DiveOperator`

    .. code-block:: python

        # Inside a TestCase where self.sensor is a custom sensor

        # Assign the desired input string to the return
        # value of the 'read' method
        attrs = {
             'read.return_value': 'output from earlier task'
        }
        self.sensor.storage = Mock(**attrs)

        # Do stuff that will call storage.read()

        # Later, we want to make sure the `read` method has been
        # called the correct number of times
        self.assertEqual(1, self.sensor.storage.read.call_count)
    """

    def get_filename(self, dag_id, task_id, execution_date):
        """
        Return an identifying path or URL to the file related to an airflow
        task instance.

        Concrete storage drivers should implement this method.

        :param str dag_id: The airflow DAG ID.
        :param str task_id: The airflow task ID.
        :param datetime.datetime execution_date: The datetime for the task
            instance.
        :return: The identifying path or URL.
        :rtype: str
        """
        raise NotImplementedError()

    def get_path(self, dag_id, task_id):
        """
        Return the path portion where files would be stored for the given task.

        This should generally be the same as get_filename except without the
        filename (execution date) portion.

        :param str dag_id: The DAG ID.
        :param str task_id: The task ID.
        :return: A path to the task's intermediate storage.
        :rtype: str
        """
        raise NotImplementedError()

    def read(self, dag_id, task_id, execution_date, encoding):
        """
        Read the data output from the given airflow task instance.

        Concrete storage drivers should implement this method.

        :param str dag_id: The airflow DAG ID.
        :param str task_id: The airflow task ID.
        :param datetime.datetime execution_date: The datetime for the task
            instance.
        :param str encoding: The encoding to use for reading in the data.
        :return: The data from the file.
        :rtype: str
        """
        raise NotImplementedError()

    def get_read_stream(self, dag_id, task_id, execution_date):
        """

        :param str dag_id: The airflow DAG ID.
        :param str task_id: The airflow task ID.
        :param datetime.datetime execution_date: The datetime for the task
            instance.
        :return:
        """
        raise NotImplementedError()

    def write(self, dag_id, task_id, execution_date, data, *args, **kwargs):
        """
        Write data to the output file identified by the airflow task instance.

        Concrete storage drivers should implement this method.

        :param str dag_id: The airflow DAG ID.
        :param str task_id: The airflow task ID.
        :param datetime.datetime execution_date: The datetime for the task
            instance.
        :param str data: The data to write.
        :param args: might be used in child classes, (currently used in
            S3StorageDriver)
        :param kwargs: same reasoning as args
        """
        raise NotImplementedError()

    def write_from_stream(self, dag_id, task_id, execution_date, stream, *args, **kwargs):
        """

        Write data to the output file identified by the airflow task instance.

        Concrete storage drivers should implement this method.

        :param str dag_id: The airflow DAG ID.
        :param str task_id: The airflow task ID.
        :param datetime.datetime execution_date: The datetime for the task
            instance.
        :param stream data: A stream to the data to write
        :param args: might be used in child classes, (currently used in
            S3StorageDriver)
        :param kwargs: same reasoning as args
        """
        raise NotImplementedError()

    def execution_date_string(self, execution_date):
        """
        Format the execution date per our standard file naming convention.

        :param datetime.datetime execution_date: The airflow task instance
            execution date.
        :return: The formatted date string.
        :rtype: str
        """
        return execution_date.strftime("%Y-%m-%d")

    def list_filenames_in_path(self, path):
        """
        Given a storage path, get all of the filenames of files directly in
        that path.

        Note that this only provides names of files and is not recursive.

        :param str path: The storage path to list.
        :return: A list of only the filename portion of filenames in the path.
        :rtype: list[str]
        """
        raise NotImplementedError()

    def list_filenames_in_task(self, dag_id, task_id):
        """
        Shortcut method to get a list of filenames stored in the given task's
        path.

        This is already implemented by depending on the unimplemented methods
        above. Override this if you need more flexibility.

        :param str dag_id: The DAG ID of the task.
        :param str task_id: The task ID.
        :return: A list of file names of files stored by the task.
        :rtype: list[str]
        """
        the_path = self.get_path(dag_id, task_id)

        return self.list_filenames_in_path(the_path)


class StorageDriverError(Exception):
    """
    Base storage driver Exception.
    """
    pass