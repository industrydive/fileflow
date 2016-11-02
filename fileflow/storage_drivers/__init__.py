from .storage_driver import StorageDriver, StorageDriverError
from .file_storage_driver import FileStorageDriver
from .s3_storage_driver import S3StorageDriver
from .. import configuration


def get_storage_driver(
        storage_type=None,
        storage_prefix=None,
        environment=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_bucket_name=None
):
    """
    Determine which intermediate storage driver to use and return it.

    Reads from the given settings to determine whether to use the local
    file system or AWS S3.

    :param str storage_type: The storage type settings. Currently supports
        'file' or 's3'.
    :param str storage_prefix: The file storage prefix. Becomes the base path
        for file storage.
    :param str environment: The environment name. Currently supported values
        are 'produciton', 'qa', 'development', and 'test'.
    :param str aws_access_key_id: AWS credential.
    :param str aws_secret_access_key: AWS credential.
    :param str aws_bucket_name: The S3 bucket name to use. Gets the
        environment name appended to it so buckets are tied to environments.
    :return: A storage driver for reading and writing intermediate data.
    :rtype: datadive.storagedrivers.StorageDriver
    """

    from fileflow.errors import FileflowError

    # Initialize all the things.
    if storage_type is None:
        storage_type = configuration.get('fileflow', 'storage_type')

    if storage_prefix is None:
        storage_prefix = configuration.get('fileflow', 'storage_prefix')

    if environment is None:
        environment = configuration.get('fileflow', 'environment')

    if aws_access_key_id is None:
        aws_access_key_id = configuration.get('fileflow', 'aws_access_key_id')

    if aws_secret_access_key is None:
        aws_secret_access_key = configuration.get('fileflow', 'aws_secret_access_key')

    if aws_bucket_name is None:
        aws_bucket_name = configuration.get('fileflow', 'aws_bucket_name')

    # Now get to the real work.
    if storage_type == 'file':
        # Here the storage prefix is used for the base path.
        return FileStorageDriver(prefix=storage_prefix)

    elif storage_type == 's3':
        full_bucket_name = aws_bucket_name
        # Given any valid environment type name aside from production,
        # append it to the provided bucket name.
        if environment in ['qa', 'development', 'test']:
            full_bucket_name += environment
        elif environment == 'production':
            # For production leave the bucket name as-is.
            pass
        else:
            raise FileflowError("ENVIRONMENT setting is net set correctly")

        return S3StorageDriver(
            access_key_id=aws_access_key_id,
            secret_access_key=aws_secret_access_key,
            bucket_name=full_bucket_name
        )

    raise FileflowError(
        'Storage driver type {} does not exist.'.format(storage_type)
    )

__all__ = ['StorageDriver', 'StorageDriverError', 'FileStorageDriver', 'S3StorageDriver', 'get_storage_driver']
