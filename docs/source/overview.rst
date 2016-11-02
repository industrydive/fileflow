Fileflow Overview
=================

Fileflow is a collection of modules that support data transfer between Airflow tasks via file targets and dependencies with either a local file system or S3 backed storage mechanism. The concept is inherited from other pipelining systems such as Make, Drake, Pydoit, and Luigi that organize pipeline dependencies with file targets. In some ways this is an alternative to Airflow's XCOM system, but supports arbitrarily large and arbitrarily formatted data for transfer whereas XCOM can only support a pickle of the size the backend database's BLOB or BINARY LARGE OBJECT implementation can allow.

Concepts
--------

The main components of fileflow are

* an operator superclass for your operators and sensors that works with multi-inheritance (:py:class:`~fileflow.operators.dive_operator.DiveOperator`)
* a task logic superclass that exposes the storage backend and convenience methods to serialize different data formats to text (:py:class:`~fileflow.task_runners.task_runner.TaskRunner`)
* two storage drivers that handle the nitty gritty of passing your serialized data to either the local file system or S3 (:py:class:`~fileflow.storage_drivers.file_storage_driver.FileStorageDriver` or :py:class:`~fileflow.storage_drivers.s3_storage_driver.S3StorageDriver`)

DiveOperator
~~~~~~~~~~~~
The ``DiveOperator`` is a subclass of :py:class:`airflow.models.BaseOperator` that mixes in the basic functionality that allows operators to define which task's data they depend on. You can subclass your own operators or sensors from ``DiveOperator`` exclusively, or mix it in via multi-inheritance with other existing operators to add the data dependency feature. For example, if you want to use the existing :py:class:`airflow.operators.PythonOperator` and mix-in smart-airflow's file targeting feature, you could define your derived class as: ::

    class DerivedOperator(DiveOperator, PythonOperator):
        ...

Given that definition, you can specify a given task's dependency on data output from an upstream task like so: ::

    a_task = DerivedOperator(
            data_dependencies = {"dependency_key": "task_id_for_dependent_task"},
             ...
             )

Smart-airflow ships with exactly this type of derived operator, which we call ``DivePythonOperator``, that works directly with the second component of smart-airflow, the ``TaskRunner``, to make file detection of upstream targets easy.

TaskRunner
~~~~~~~~~~
The ``TaskRunner`` is a superclass for you to use to define your task logic within your subclass's ``run()`` method. Many of the Airflow examples set up the task logic as plain functions that the ``PythonOperator`` calls; however our ``DivePythonOperator`` instead expects the class name of a subclass of ``TaskRunner`` and during operator execution will call ``TaskRunner.run()`` which should contain the actual logic of the task. To be more clear, see this example comparing the basic :py:class:`~fileflow.operators.python_operator.PythonOperator` signature and our :py:class:`~fileflow.operators.dive_python_operator.DivePythonOperator` signature: ::

        # vanilla airflow PythonOperator
        normal_task = PythonOperator(
                        task_id="some_unique_task_id",
                        python_callable=a_function_name,
                        dag=dag)

        # fancy DivePythonOperator
        fancy_task = DivePythonOperator(
                        task_id="some_other_unique_task_id",
                        python_object=AClassNameThatSubclassesTaskRunnerAndHasARunMethod,
                        data_dependencies={"important_data": normal_task.task_id},
                        dag=dag)

If you're a snowflake and don't like calling your main logic wrapper ``run()`` and, for example, want to call it ``ninja_move()``, you can configure that on the operator in the DAG: ::

        # fancy DivePythonOperator for someone who wants to be unique
        fancy_and_unique_task = DivePythonOperator(
                        task_id="yet_aother_unique_task_id",
                        python_object=AClassNameThatSubclassesTaskRunnerAndHasANinjaMoveMethod,
                        python_methode="ninja_move",
                        data_dependencies={"important_data": normal_task.task_id},
                        dag=dag)

All of this is to take advantage of the fact that we've done a bunch of work in ``TaskRunner`` to give it the ability to easily pass forward Airflow specific details to the storage driver to determine where it should write its target or where its upstream task's wrote their targets. We've also written into ``TaskRunner`` several serialization methods that can serialize different file formats such as JSON, pandas DataFrames, and bytestreams for convenience. The idea is that by the time the ``TaskRunner`` has passed off some data to the appropriate storage driver, the data is already serialized into a single ``str`` representation or ``BytesIO`` object.

storage drivers
~~~~~~~~~~~~~~~

The two storage drivers shipped in ``smart-airflow`` deal with the nitty gritty of actually communicating with either the local file system in the case of :py:class:`~fileflow.storage_drivers.file_storage_driver.FileStorageDriver`, or with an S3 bucket in the case of :py:class:`~fileflow.storage_drivers.s3_storage_driver.S3StorageDriver`. The storage driver needs to be able to

* derive a path or key name or names from the Airflow TaskInstance context data passed through by the TaskRunner for either upstream tasks (data dependencies) or the current task's target
* read and write to that path or key name

Since we're working with text I/O obviously this introduces a bunch of decisions the storage drivers have to be making regarding encoding/charsets, file read/write mode, path/key existence, and in the case of putting to S3 over HTTP, content types. All of this is handled by the respective storage driver; the interface for what a storage driver should implement is represented by the base :py:class:`~fileflow.storage_drivers.storage_driver.StorageDriver` class.



