import logging
from datetime import datetime, timedelta

from airflow import DAG

from fileflow.operators import DivePythonOperator
from fileflow.task_runners import TaskRunner


# Define the logic for your tasks as classes that subclasses from TaskRunner
# By doing so it will have access to TaskRunner's convenience methods to read and write files

# Here's an easy one that just writes a file.
class TaskRunnerExample(TaskRunner):
    def run(self, *args, **kwargs):
        output_string = "This task -- called {} -- was run.".format(self.task_instance.task_id)
        self.write_file(output_string)
        logging.info("Wrote '{}' to '{}'".format(output_string, self.get_output_filename()))


# Here's a more complicated one that will read the file from its upstream task, do something, and then write its own file
# It also shows you how to override __init__ if you need to
class TaskRunnerReadExample(TaskRunner):
    def __init__(self, context):
        """
        An example how to write the init on a class derived from TaskRunner
        :param context: Required.
        """
        super(TaskRunnerReadExample, self).__init__(context)
        self.output_template = "Read '{}' from '{}'. Writing output to '{}'."


    def run(self, *args, **kwargs):
        # This is how you read the output of a previous task
        # The argument to read_upstream_file is based on the DAG configuration
        input_string = self.read_upstream_file("something")

        # An example bit of 'logic'
        output_string = self.output_template.format(
            input_string,
            self.get_input_filename("something"),
            self.get_output_filename()
        )

        # And write out the results of the logic to the correct file
        self.write_file(output_string)

        logging.info(output_string)


# Now let's define a DAG
dag = DAG(
    dag_id='fileflow_example',
    start_date=datetime(2030, 1, 1),
    schedule_interval=timedelta(minutes=1)
)

# The tasks in this DAG will use DivePythonOperator as the operator, which knows how to send a TaskRunner anything in the `data_dependencies` keyword so you can specify more than one file by name to be fed to a downstream task
t1 = DivePythonOperator(
    task_id="write_a_file",
    python_method="run",
    python_object=TaskRunnerExample,
    provide_context=True,
    owner="airflow",
    dag=dag
)

# We COULD set `python_method="run"` here as above, but "run" is the
# default value, so we're not bothering to set it
t2 = DivePythonOperator(
    task_id="read_that_file",
    python_object=TaskRunnerReadExample,
    data_dependencies={"something": t1.task_id},
    # remember how our TaskRunner subclass knows how to read the upstream file with the key 'something'? This is why
    provide_context=True,
    owner="airflow",
    dag=dag
)

