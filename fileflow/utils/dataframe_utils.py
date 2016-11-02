"""
.. module:: utils.dataframe_utils
   :synopsis: Utility functions for reading, writing and cleaning pandas dataframes

.. moduleauthor:: David Barbarisi <dbarbarisi@industrydive.com>
.. moduleauthor:: Donald Vetal <dvetal@industrydive.com>
.. moduleauthor:: Laura Lorenz <llorenz@industrydive.com>
.. moduleauthor:: Miriam Sexton <miriam@industrydive.com>
"""

import csv
import logging
import pandas as pd


def read_and_clean_csv_to_dataframe(filename_or_stream, encoding='utf-8'):
    """
    Reads a utf-8 encoded CSV directly into a pandas dataframe as string values and scrubs np.NaN values to Python None

    :param str filename_or_stream: path to CSV
    :return:
    """
    # pulls data in as utf8, all as strings, and without pre whitespace padding
    try:
        data = pd.read_csv(
            filepath_or_buffer=filename_or_stream,
            encoding=encoding,
            dtype=str,
            skipinitialspace=True
        )
    except AttributeError:
        # this is an empty dataframe and pandas crashed because it can't coerce the columns to strings
        # issue and PR to fix is open on pandas core at https://github.com/pydata/pandas/issues/12048
        # slated for 1.8 release
        # so for now just try loading the dataframe without specifying dtype
        data = pd.read_csv(
            filepath_or_buffer=filename_or_stream,
            encoding=encoding,
            skipinitialspace=True
        )
    logging.info('File read via the pandas read_csv methodology.')

    # coerces pandas nulls (of np.NaN type) into python None
    data = data.where((pd.notnull(data)), None)

    # coerces string representations of Python None to a real Python None
    data[data == 'None'] = None
    data[data == ''] = None
    logging.info("Dataframe of shape %s has been retrieved." % str(data.shape))

    return data


def clean_and_write_dataframe_to_csv(data, filename):
    """
    Cleans a dataframe of np.NaNs and saves to file via pandas.to_csv

    :param data: data to write to CSV
    :type data: :class:`pandas.DataFrame`
    :param filename: Path to file to write CSV to. if None, string of data
        will be returned
    :type filename: str | None
    :return: If the filename is None, returns the string of data. Otherwise
        returns None.
    :rtype: str | None
    """
    # cleans np.NaN values
    data = data.where((pd.notnull(data)), None)
    # If filename=None, to_csv will return a string
    result = data.to_csv(path_or_buf=filename, encoding='utf-8', dtype=str, index=False, na_rep=None,
                         skipinitialspace=True, quoting=csv.QUOTE_ALL)
    logging.info("Dataframe of shape %s has been stored." % str(data.shape))

    return result



