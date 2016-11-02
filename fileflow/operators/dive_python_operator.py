"""
.. module:: operators.dive_operator
    :synopsis: DivePythonOperator for use with TaskRunner

.. moduleauthor:: Laura Lorenz <llorenz@industrydive.com>
.. moduleauthor:: Miriam Sexton <miriam@industrydive.com>
"""

from airflow.operators import PythonOperator

from .dive_operator import DiveOperator


class DivePythonOperator(DiveOperator, PythonOperator):
    """
    Python operator that can send along data dependencies to its callable.
    Generates the callable by initializing its python object and calling its method.
    """

    def __init__(self, python_object, python_method="run", *args, **kwargs):
        self.python_object = python_object
        self.python_method = python_method
        kwargs['python_callable'] = None

        super(DivePythonOperator, self).__init__(*args, **kwargs)

    def pre_execute(self, context):
        context.update(self.op_kwargs)
        context.update({"data_dependencies": self.data_dependencies})
        instantiated_object = self.python_object(context)
        self.python_callable = getattr(instantiated_object, self.python_method)
