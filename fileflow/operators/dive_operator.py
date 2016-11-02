"""
.. module:: operators.dive_operator
    :synopsis: Base fileflow operator: DiveOperator

.. moduleauthor:: Laura Lorenz <llorenz@industrydive.com>
.. moduleauthor:: Miriam Sexton <miriam@industrydive.com>
"""

from airflow.operators import BaseOperator

from fileflow.storage_drivers import get_storage_driver

class DiveOperator(BaseOperator):
    """
    An operator that sets up storage and assigns data dependencies to the
    operator class.

    This is intended as a base class for eg :py:class:`fileflow.operators.DivePythonOperator`.
    """

    def __init__(self, *args, **kwargs):
        super(DiveOperator, self).__init__(*args, **kwargs)

        # The upstream dependencies
        # These must always be specified
        # Dictionary can contain any number of keys which must be redirected
        # in the business logic to their read/parse methods.
        self.data_dependencies = kwargs.pop('data_dependencies', {})

        # private storage driver attribute; lazy loaded by the storage property or set by the storage property setter
        self._storage = None

    @property
    def storage(self):
        """
        Lazy load a storage property on access instead of on class instantiation. Something in the storage attribute
        is not deep-copyable which causes errors with airflow clear and airflow backfill which both try to deep copy
        a target DAG and all its operators, so we only want this property when we actually use it.
        """
        if self._storage is None:
            self._storage = get_storage_driver()
        return self._storage

    @storage.setter
    def storage(self, value):
        """
        Allow the storage property to be set, in particularly for tests that mock this property with a Mock object.

        :param value: Value to set this object's storage property to.
        """
        self._storage = value

