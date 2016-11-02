# fileflow

Fileflow is a collection of modules that support data transfer between Airflow tasks via file targets and dependencies with either a local file system or S3 backed storage mechanism. The concept is inherited from other pipelining systems such as Make, Drake, Pydoit, and Luigi that organize pipeline dependencies with file targets. In some ways this is an alternative to Airflow's XCOM system, but supports arbitrarily large and arbitrarily formatted data for transfer whereas XCOM can only support a pickle of the size the backend database's BLOB or BINARY LARGE OBJECT implementation can allow.

### Resources

- Read the docs at [readthedocs.io](http://fileflow.readthedocs.io/en/latest/).
- Learn about why Industry Dive chose to make fileflow with [this video from PyData DC 2016](https://www.youtube.com/watch?v=60FUHEkcPyY&index=35&list=PLGVZCDnMOq0qLoYpkeySVtfdbQg1A_GiB) given by contributor @lauralorenz

### Contributors

- @lauralorenz
- @MiriamSexton
- @dbarbar
- @dvetal