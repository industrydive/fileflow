"""
Extend from the airflow configuration and address any missing fileflow related configuration values.
"""

from airflow import configuration as airflow_configuration
import os
import boto

def _ensure_section_exists(section_name):
    """
    Checks to make sure the config has a section called section_name. If it doesn't, create one.

    EXPLANATION:
    conf is a singleton in airflow configuration ("conf"). It is an object of a custom airflow class
    that is a ConfigParser subclass.
    The proper way to access the config is through the functions defined in airflow.configuration which
    pass the call the singleton. (Ex: airflow.configuration.get calls conf.get). However, not all methods on conf
    are abstracted out, including the all important "has_section" & "add_section" (if we try to set an option
    on a section that doesn't exist, NoSectionError is raised). Therefore, we break the abstraction here once to
    access the singleton directly

    :param str section: the section
    """
    # This uses the singleton described above to make sure the section exists
    if not airflow_configuration.conf.has_section(section_name):
        airflow_configuration.conf.add_section(section_name)

_ensure_section_exists('fileflow')

# Set some fileflow settings to a default if they do not already exist.
if not airflow_configuration.has_option('fileflow', 'environment'):
    airflow_configuration.set('fileflow', 'environment', 'production')

if not airflow_configuration.has_option('fileflow', 'storage_prefix'):
    airflow_configuration.set('fileflow', 'storage_prefix', 'storage')

if not airflow_configuration.has_option('fileflow', 'storage_type'):
    airflow_configuration.set('fileflow', 'storage_type', 'file')

if not airflow_configuration.has_option('fileflow', 'aws_bucket_name'):
    airflow_configuration.set('fileflow', 'aws_bucket_name', 'mybeautifulbucket')

# For AWS keys, check the AIRFLOW__ style environment variables first
# Otherwise, fallback to the boto configuration
aws_access_key_id_env_var = os.environ.get('AIRFLOW__FILEFLOW__AWS_ACCESS_KEY_ID', False)
aws_secret_access_key_env_var = os.environ.get('AIRFLOW__FILEFLOW__AWS_SECRET_ACCESS_KEY', False)
boto_config = boto.pyami.config.Config()

if not airflow_configuration.has_option('fileflow', 'aws_access_key_id'):
    if aws_access_key_id_env_var:
        airflow_configuration.set('fileflow', 'aws_access_key_id', aws_access_key_id_env_var)
    else:
        boto_aws_access_key_id_default = boto_config.get('Credentials', 'aws_access_key_id')
        airflow_configuration.set('fileflow', 'aws_access_key_id', boto_aws_access_key_id_default)

if not airflow_configuration.has_option('fileflow', 'aws_secret_access_key'):
    if aws_secret_access_key_env_var:
        airflow_configuration.set('fileflow', 'aws_secret_acccess_key', aws_secret_access_key_env_var)
    else:
        boto_aws_secret_access_key_default = boto_config.get('Credentials', 'aws_secret_access_key')
        airflow_configuration.set('fileflow', 'aws_secret_acccess_key', boto_aws_secret_access_key_default)


def get(section, key, **kwargs):
    """
    Expose the underlying airflow configuration object from the fileflow configuration module.

    :param str section: Section title in airflow.cfg you're looking for
    :param str key: Key in the given section in airflow.cfg you're looking for
    :param kwargs: Not expected
    :return:
    """
    # traversing through the airflow configuration module (aliased here as airflow_configuration)
    # to the actual ConfigParser subclass (conf)
    # to get to it's get() method
    return airflow_configuration.conf.get(section, key, **kwargs)

