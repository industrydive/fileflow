from unittest import TestCase
from airflow.models import TaskInstance
from nose.plugins.attrib import attr
from datetime import datetime
import mock
import tempfile

from fileflow.task_runners import TaskRunner


@attr('unittest')
class TestTaskRunner(TestCase):
    """
    The general idea behind this set of tests is to make sure the api works
    and calls the correct storage methods and understands how to parse the data_dependencies.
    Therefore, these tests assume the storage driver works and focus on making sure it
    (or a method that calls it) is called with the correct arguments. Mock is used a lot
    here.
    """
    def setUp(self):
        # Set up arguments for a :py:class:`airflow.models.TaskInstance`
        # that will be sent to the :py:class:`datadive.smart-airflow.DivePythonOperator` class
        self.execution_date = datetime(2015, 1, 1)
        self.dag_id = 'test_taskrunner_task_instance'
        self.task_id = 'fake_task_id'

        # normally an operator or sensor, when called by the scheduler, instantiates a :py:class:`airflow.models.TaskInstance`
        # that inherits from the operator what logic it should execute, what dag_id and task_id namespace it is under,
        # and for what execution_date
        # Here we mock a fake :py:class:`datadive.smart_airflow.TaskRunner`-derivative (``self.fake_task``)
        # so we can instantiate a :py:class:`airflow.models.TaskInstance` with it
        self.fake_task = mock.MagicMock()
        self.fake_task.dag_id = self.dag_id
        self.fake_task.task_id = self.task_id
        task_instance = TaskInstance(self.fake_task, self.execution_date)

        # build the context dictionary for this task and instantiate a :py:class:`datadive.smart_airflow.TaskRunner` with it
        # this is normally handled during an :py:class:`airflow.models.BaseOperator`-derived class's :py:meth:`pre_execute` method
        # but here we instead instantiate our own task runner instance in the way divepythonoperator normally would
        # so that we can run its own methods ourselves during the tests
        self.context = {
            'execution_date': self.execution_date,
            'ti': task_instance,
            # These data dependencies are used for tests in several places to check arg passing between TaskRunner conveience methods
            # and the storage driver
            'data_dependencies': {'dep_one': 'task_one', 'dep_two': 'task_two'},
        }
        self.task_runner_instance = TaskRunner(self.context)

        # mock out the storage driver; we won't be testing the storage driver directly in this series of tests
        self.task_runner_instance.storage = mock.MagicMock()

    def test_creation(self):
        """
        Assert we can instantiate a TaskRunner under different simulated situations.
        """

        # The TaskInstance must be sent or we won't be able to infer dag_id and task_id
        with self.assertRaises(KeyError) as no_ti:
            TaskRunner({'execution_date': self.execution_date})
        self.assertEqual(no_ti.exception.args[0], 'ti')

        # The execution_date must be sent or we won't be able to know what execution_date the file is for
        with self.assertRaises(KeyError) as no_date:
            TaskRunner({'ti': 'fake'})
        self.assertEqual(no_date.exception.args[0], 'execution_date')

        # This should not raise an exception; during initialization we do no type checks
        # This a loophole we exploit in other tests and will be "fixed" by version 1.0
        # TODO: alternate execution_date type?
        # TODO: do we even want this
        TaskRunner({"ti": 'fake', 'execution_date': self.execution_date})

    def test_run(self):
        """
        Assert TaskRunner's specify an interface for the run() method via a NotImplementedError exception.
        """
        with self.assertRaises(NotImplementedError):
            self.task_runner_instance.run()

    @mock.patch('fileflow.task_runners.task_runner.datetime')
    def test_write_timestamp_file(self, mock_dt):
        """
        Assert a TaskRunner calls write_json with the expected args. Implicitly testing that we are using datetime.isoformat to derive
        the value for the "RUN" key.

        :param mock_dt: ??? not sure this patch is necessary
        """
        fake_date_str = "not iso formatted"
        mock_datetime_obj = mock.MagicMock()
        mock_datetime_obj.isoformat = mock.MagicMock(return_value=fake_date_str)
        mock_dt.datetime.now = mock.MagicMock(return_value=mock_datetime_obj)

        self.task_runner_instance.write_json = mock.MagicMock()
        self.task_runner_instance.write_timestamp_file()

        self.task_runner_instance.write_json.assert_called_once_with({'RUN': fake_date_str})

    def test_get_input_filename(self):
        """
        Assert we can parse the data_dependencies dict and pass the relevant data needed to infer the upstream task's output file location
        correctly to the storage driver.

        This is implicitly testing that the airflow.models.TaskInstance model in the context of the TaskRunner has the same right
        properties as we passed it in the setUp() method and that the contract between us and the TaskInstance model hasn't changed.
        """

        # Test a call without setting the dag_id
        self.task_runner_instance.get_input_filename('dep_one')
        self.task_runner_instance.storage.get_filename.assert_called_once_with(
            self.dag_id,
            'task_one',
            self.execution_date
        )
        self.task_runner_instance.storage.get_filename.reset_mock()

        # And test a call where we do set the dag id to something different
        new_dag_id = 'some_dag_id'
        self.task_runner_instance.get_input_filename('dep_two', new_dag_id)
        self.task_runner_instance.storage.get_filename.assert_called_once_with(
            new_dag_id,
            'task_two',
            self.execution_date
        )

    def test_get_output_filename(self):
        """
        Assert we can parse the current :py:class:`datadive.smart_airflow.TaskRunner` to pass relevant data needed to infer the this task's
        output file location correctly to the storage driver.
        """
        self.task_runner_instance.get_output_filename()
        self.task_runner_instance.storage.get_filename.assert_called_once_with(
            self.dag_id,
            self.task_id,
            self.execution_date
        )

    def test_get_upstream_stream(self):
        """
        Assert seek works, that get_upstream_stream resets the stream at pos 0, and that args passed to get_upstream_stream forwards
        to the storage driver's get_read_stream method.
        """
        temp_stream = tempfile.TemporaryFile()
        some_string = 'abcde'
        temp_stream.write(some_string)

        mock_reader = mock.MagicMock()
        mock_reader.return_value = temp_stream
        self.task_runner_instance.storage.get_read_stream = mock_reader

        # Call without any optional args
        result = self.task_runner_instance.get_upstream_stream('dep_one')
        mock_reader.assert_called_once_with(self.dag_id, 'task_one', self.execution_date)
        # Make sure the stream position has been moved back to the beginning
        self.assertEqual(0, result.tell())

        # move the position in the stream back so we can test again
        # that it was reset to zero by get_upstream_stream
        temp_stream.seek(2)
        # and reset the mock
        mock_reader.reset_mock()

        # And try setting the dag explicitly
        new_dag_id = 'some_other_dag_id'
        result = self.task_runner_instance.get_upstream_stream('dep_two', new_dag_id)
        mock_reader.assert_called_once_with(new_dag_id, 'task_two', self.execution_date)
        self.assertEqual(0, result.tell())

    def test_read_upstream_file(self):
        """
        Assert we forward convenience method read_upstream_file and its args to the storage driver's read method.
        """
        # For readability
        mock_read = mock.MagicMock()
        self.task_runner_instance.storage.read = mock_read

        # Test once with the default arguments
        self.task_runner_instance.read_upstream_file('dep_one')
        mock_read.assert_called_once_with(
            self.dag_id,
            'task_one',
            self.execution_date,
            encoding='utf-8'
        )
        mock_read.reset_mock()

        # Test with a different dag id
        new_dag_id = 'another_fake_dag_id'
        self.task_runner_instance.read_upstream_file('dep_one', new_dag_id)
        mock_read.assert_called_once_with(
            new_dag_id,
            'task_one',
            self.execution_date,
            encoding='utf-8'
        )
        mock_read.reset_mock()

        # Test with a different encoding
        self.task_runner_instance.read_upstream_file('dep_two', encoding='not-a-real-encoding')
        mock_read.assert_called_once_with(
            self.dag_id,
            'task_two',
            self.execution_date,
            encoding='not-a-real-encoding'
        )
        mock_read.reset_mock()

        # And once with a different dag AND a weird encoding
        self.task_runner_instance.read_upstream_file('dep_two', encoding='fake', dag_id=new_dag_id)
        mock_read.assert_called_once_with(
            new_dag_id,
            'task_two',
            self.execution_date,
            encoding='fake'
        )

    @mock.patch('fileflow.task_runners.task_runner.read_and_clean_csv_to_dataframe')
    def test_read_upstream_pandas_csv(self, mock_csv_reader):
        """
        Test this method signature by sending it varying arguments we expect it can take

        We assume get_upstream_stream works correctly (because we are testing it separately in test_get_upstream_stream)
        so we do not need to follow the api contract all the way to the storage driver. We likewise assume
        read_and_clean_csv_to_dataframe works correctly, because it has its own tests as well. For this test we only have
        to make sure the read_upstream_pandas_csv arguments are being passed through to get_upstream_stream correctly.

        :param mock_csv_reader: mock of read_and_clean_csv_to_dataframe
        """
        # For readability
        fake_stream = 'not a stream'
        mock_get_stream = mock.MagicMock()
        mock_get_stream.return_value = fake_stream
        self.task_runner_instance.get_upstream_stream = mock_get_stream

        # Test once with the default arguments
        self.task_runner_instance.read_upstream_pandas_csv('dep_one')
        mock_get_stream.assert_called_once_with('dep_one', None)
        print 'assert called onse a'
        mock_csv_reader.assert_called_once_with(
            filename_or_stream=fake_stream,
            encoding='utf-8'
        )
        mock_get_stream.reset_mock()
        mock_csv_reader.reset_mock()

        # Test with a different dag id
        new_dag_id = 'another_fake_dag_id'
        self.task_runner_instance.read_upstream_pandas_csv('dep_one', new_dag_id)
        mock_get_stream.assert_called_once_with('dep_one', new_dag_id)
        mock_csv_reader.assert_called_once_with(
            filename_or_stream=fake_stream,
            encoding='utf-8'
        )
        mock_get_stream.reset_mock()
        mock_csv_reader.reset_mock()

        # Test with a different encoding
        self.task_runner_instance.read_upstream_pandas_csv('dep_one', encoding='not-a-real-encoding')
        mock_get_stream.assert_called_once_with('dep_one', None)
        mock_csv_reader.assert_called_once_with(
            filename_or_stream=fake_stream,
            encoding='not-a-real-encoding'
        )
        mock_get_stream.reset_mock()
        mock_csv_reader.reset_mock()

        # And once with a different dag AND a weird encoding
        self.task_runner_instance.read_upstream_pandas_csv('dep_one', new_dag_id, 'bad-encoding')
        mock_get_stream.assert_called_once_with('dep_one', new_dag_id)
        mock_csv_reader.assert_called_once_with(
            filename_or_stream=fake_stream,
            encoding='bad-encoding'
        )
        mock_get_stream.reset_mock()
        mock_csv_reader.reset_mock()

    @mock.patch('fileflow.task_runners.task_runner.json.loads')
    def test_read_upstream_json(self, mock_json_loads):
        """
        Assert convenience method read_upstream_json signature by sending it varying arguments we expect it can take.

        We assume read_upstream_file works correctly (because we are testing it separately in test_read_upstream_file)
        so we do not need to follow the api contract all the way to the storage driver. We just have to make sure the
        read_upstream_json arguments are being passed through to read_upstream_file correctly. We also test that the
        standard json.loads is getting called, not some hacky attempt at parsing json.

        :param mock_json_loads: Mock object preventing actual loads since we pass our own fake data here
        """
        # Want to make sure we're really calling json.loads
        # Not doing anything weird
        fake_result = 'fake result that is not json'
        mock_read_upstream = mock.MagicMock(return_value=fake_result)
        self.task_runner_instance.read_upstream_file = mock_read_upstream

        # Test once with the default arguments
        self.task_runner_instance.read_upstream_json('dep_one')
        mock_read_upstream.assert_called_once_with('dep_one', None, encoding='utf-8')
        mock_json_loads.assert_called_once_with(fake_result)
        mock_read_upstream.reset_mock()
        mock_json_loads.reset_mock()

        # Test with a different dag id
        new_dag_id = 'another_fake_dag_id'
        self.task_runner_instance.read_upstream_json('dep_one', new_dag_id)
        mock_read_upstream.assert_called_once_with('dep_one', new_dag_id, encoding='utf-8')
        mock_json_loads.assert_called_once_with(fake_result)
        mock_read_upstream.reset_mock()
        mock_json_loads.reset_mock()

        # Test with a different encoding
        self.task_runner_instance.read_upstream_json('dep_one', encoding='not-a-real-encoding')
        mock_read_upstream.assert_called_once_with('dep_one', None, encoding='not-a-real-encoding')
        mock_json_loads.assert_called_once_with(fake_result)
        mock_read_upstream.reset_mock()
        mock_json_loads.reset_mock()

        # And once with a different dag AND a weird encoding
        self.task_runner_instance.read_upstream_json('dep_one', new_dag_id, 'bad-encoding')
        mock_read_upstream.assert_called_once_with('dep_one', new_dag_id, encoding='bad-encoding')
        mock_json_loads.assert_called_once_with(fake_result)
        mock_read_upstream.reset_mock()
        mock_json_loads.reset_mock()

    def test_write_file(self):
        """
        Assert we forward convenience method write_file and its args to the storage driver's write method.
        """
        mock_writer = mock.MagicMock()
        self.task_runner_instance.storage.write = mock_writer

        fake_data = 'here is some data yay!'

        # Test once with the default arguments
        self.task_runner_instance.write_file(fake_data)
        mock_writer.assert_called_once_with(
            self.dag_id,
            self.task_id,
            self.execution_date,
            fake_data,
            content_type='text/plain'
        )
        mock_writer.reset_mock()

        # Test with a different content_type
        self.task_runner_instance.write_file(fake_data, 'special/unreadable')
        mock_writer.assert_called_once_with(
            self.dag_id,
            self.task_id,
            self.execution_date,
            fake_data,
            content_type='special/unreadable'
        )
        mock_writer.reset_mock()

    def test_write_from_stream(self):
        """
        Assert we forward convenience method write_from_stream and its args to the storage driver's write_from_stream method.
        """
        mock_writer = mock.MagicMock()
        self.task_runner_instance.storage.write_from_stream = mock_writer
        fake_stream = 'not a stream'

        # Test with default content type
        self.task_runner_instance.write_from_stream(fake_stream)
        mock_writer.assert_called_once_with(
            self.dag_id,
            self.task_id,
            self.execution_date,
            fake_stream,
            content_type='text/plain'
        )
        mock_writer.reset_mock()

        # And test with a custom content type
        self.task_runner_instance.write_from_stream(fake_stream, 'mytype')
        mock_writer.assert_called_once_with(
            self.dag_id,
            self.task_id,
            self.execution_date,
            fake_stream,
            content_type='mytype'
        )

    @mock.patch('fileflow.task_runners.task_runner.clean_and_write_dataframe_to_csv')
    def test_write_pandas_csv(self, mock_csv_writer):
        """
        Assert we forward from convenience method write_pandas_csv and its args to the storage driver's write_file method.

        We assume write_file works correctly (because we are testing it separately in test_write_file)
        so we do not need to follow the api contract all the way to the storage driver. We likewise assume
        clean_and_write_dataframe_to_csv works correctly, because it also has its own tests. For this test we only have to
        test that the write_pandas_csv arguments are being passed through to write_file correctly.

        :param mock_csv_writer: mock for our method that serializes a dataframe
        """
        fake_csv = 'this is not at all a csv'
        mock_csv_writer.return_value = fake_csv
        mock_writer = mock.MagicMock()
        self.task_runner_instance.write_file = mock_writer

        self.task_runner_instance.write_pandas_csv('fake data')
        mock_csv_writer.assert_called_once_with(data='fake data', filename=None)
        mock_writer.assert_called_once_with(fake_csv, content_type='text/csv')

    @mock.patch('json.dumps')
    def test_write_json(self, mock_json_dumps):
        """
        Assert we forward from convenience method write_json and its args to the storage driver's write_file method. Also
        test that the standard json.dumps is called.

        :param mock_json_dumps: mock for when this convenience method calls json.dumps to serialize the json
        """
        mock_json_dumps.return_value = "fake"
        mock_writer = mock.MagicMock()
        self.task_runner_instance.write_file = mock_writer

        fake_data = {'val': 'key', 'val2': [1, 2]}
        self.task_runner_instance.write_json(fake_data)
        mock_json_dumps.assert_called_once_with(fake_data)
        mock_writer.assert_called_once_with("fake", content_type='application/json')
