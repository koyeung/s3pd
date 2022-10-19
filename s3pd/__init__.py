import mmap
import os
import contextlib
from tempfile import NamedTemporaryFile
from multiprocessing import current_process
import multiprocessing.pool
from urllib.parse import urlparse
from io import BytesIO

import botocore
import boto3


LINK_SENTINEL = '#S3LINK#'

@contextlib.contextmanager
def prepare_shared_file(size, destination):
    """Return path of shared file.

    :param size: Size of the file to create, in bytes.
    :param destination: Path of the file to create, or `None` to create a
        temporary file in /dev/shm (available on linux only).
    :return: path to be used from a context manager.
    """
    if destination:
        with open(destination, mode='w+b') as shmfile:
            os.truncate(shmfile.fileno(), size)
            shmfile.seek(0)
            yield destination
    else:
        with NamedTemporaryFile(mode='wb', prefix='s3-', dir='/dev/shm') as shmfile:
            os.truncate(shmfile.fileno(), size)
            shmfile.seek(0)
            yield shmfile.name


def get_filesize(client, bucket, key, version=None):
    """Return the size of file on S3.

    :param client: The client to use for performing the query.
    :param bucket: Name of the S3 bucket.
    :param key: Path inside the bucket (without leading `/`)
    :param version: The file version to retrieve, or None
    :return: The file size, in bytes.
    """
    args = {
        'Bucket': bucket,
        'Key': key,
        **({'VersionId': version} if version else {}),
    }
    return client.head_object(**args)['ContentLength']

def create_chunks(chunksize, filesize):
    """Generate list of constant size chunks from a filesize.

    The chunksize should be a multiple of mmap.ALLOCATIONGRANULARITY (most
    likely 4KB). Example:

        >>> create_chunks(5, 12)
        [(0, 4), (5, 9), (10, 11)]

    :param chunksize: Desired chunk size, in bytes.
    :param filesize: Provided file size, in bytes.
    :return: A list of tuples `(offset first, offset last)` for every chunk
        of the file. Each offset will have exactly the same size (`chunksize`)
        except of course the last one if the file size is not divisible by
        the chunk size.
    """
    return [
        (i, min(i+chunksize-1, filesize-1)) for i in range(0, filesize, chunksize)]

def create_client(signed=True):
    """Create a boto client.

    :param signed: If `False` return a client not signing requests.
    :return: The `boto3.Client`.
    """
    if signed:
        return boto3.client('s3')
    else:
        return boto3.client(
            's3',
            config=botocore.config.Config(signature_version=botocore.UNSIGNED))

def download_chunk(
        bucket, key, shared_filepath, offset_first, offset_last, signed, version=None):
    """Worker function to download a chunk of the file.

    :param bucket: Name of the S3 bucket.
    :param key: Path inside the bucket (without leading `/`)
    :param shared_filepath: path to shared file.
    :param offset_first: Start position of the chunk to download.
    :param offset_last: Last position of the chunk to download.
    :param version: The file version to retrieve, or None
    :return: Nothing, the chunk is directly copied in the destination file.
    """
    client = create_client(signed)
    args = {
        'Bucket': bucket,
        'Key': key,
        'Range': 'bytes=%s-%s' % (offset_first, offset_last),
        **({'VersionId': version} if version else {}),
    }

    offset = offset_first
    assert offset % mmap.ALLOCATIONGRANULARITY == 0
    length = offset_last - offset_first + 1

    with open(shared_filepath, mode='a+b') as file_, mmap.mmap(
            fileno=file_.fileno(),
            length=length,
            offset=offset,
    ) as shmmap:
        chunk = client.get_object(**args)['Body']
        chunk._raw_stream.readinto(shmmap)


def resolve_link(bucket, key, client, depth=10):
    """Resolve S3 links to target key.

    :param bucket: Name of the S3 bucket.
    :param key: Path inside the bucket (without leading `/`)
    :param client: boto3 client to use when performing requests.
    :param depth: Maximum number of link indirections before stopping.
    """
    # Stop after too many link indirections
    assert depth > 0, 'Too many levels of link indirections'

    new_bucket, new_key = check_link_target(bucket, key, client)
    if new_bucket == bucket and new_key == key:
        return new_bucket, new_key

    return resolve_link(
        bucket=new_bucket,
        key=new_key,
        client=client,
        depth=depth-1)


def check_link_target(bucket, key, client):
    filesize = get_filesize(client, bucket, key)
    if filesize > 1024:
        return bucket, key

    with BytesIO() as stream:
        client.download_fileobj(Bucket=bucket, Key=key, Fileobj=stream)
        # In case decoding utf-8 fails, then we are not in a presence of a link
        try:
            content = stream.getvalue().decode("utf-8").strip()
        except UnicodeDecodeError:
            return bucket, key

    # Check whether this file is a link
    if not content.startswith(LINK_SENTINEL):
        return bucket, key

    url = content[len(LINK_SENTINEL) :]
    parsed_url = urlparse(url)
    path = parsed_url.path

    # In case the link url ommits the s3://bucket/ part, then
    # assume it is a key relative to the current bucket
    new_bucket = parsed_url.netloc or bucket

    # S3 keys do not start with /
    new_key = path if not path.startswith('/') else path[1:]

    return new_bucket, new_key


def s3pd(
        url, processes=8, chunksize=67108864, destination=None, func=None,
        signed=True, version=None, mp_context=None):
    """Main entry point to download an s3 file in parallel.

    Example to download a file locally:
        >>> s3pd(url='s3://bucket/file.txt', destination='/tmp/file.txt')

    Example to load an HDF5 file from S3:
        >>> df = s3pd(url='s3://bucket/file.h5', func=pd.read_hdf)

    :param url: S3 address of the file to download, using the 's3' scheme
        such as `s3://bucket-name/file/to/download.txt`.
    :param processes: Number of processes to use for the download, default
        to 8. Forced to 1 if not the main process when using multiprocessing.
    :param chunksize: Size of each chunk to download, default to 64MB.
    :param destination: Destination path for the downloaded file, including the
        filename. If `None`, a temporary file is created in /dev/shm and you
        should provide a `func` to apply on the filename and return. This is
        useful if just want to apply a function (e.g. loading) on a remote
        file.
    :param mp_context: multiprocessing context object for starting
        worker processes. If not provided, OS dependent default start method would
        be used.
    """
    assert chunksize % mmap.ALLOCATIONGRANULARITY == 0

    parsed_url = urlparse(url)
    bucket = parsed_url.netloc
    key = parsed_url.path[1:]
    client = create_client(signed)

    bucket, key = resolve_link(bucket, key, client)

    filesize = get_filesize(client, bucket, key, version=version)
    chunks = create_chunks(chunksize, filesize)

    # Don't fork more children than chunks
    processes = min(processes, len(chunks))

    # Prevent multiprocessing children to fork
    if current_process().daemon:
        processes = 1

    with prepare_shared_file(filesize, destination) as shared_filepath:
        download_tasks = [
            # pass path to shared file instead of file descriptor
            # for use with multiprocessing context start method other than fork
            (bucket, key, shared_filepath, *chunk, signed, version)
            for chunk in chunks]

        if processes == 1:
            for task in download_tasks:
                download_chunk(*task)
        else:
            with multiprocessing.pool.Pool(
                    processes=processes, context=mp_context
            ) as pool:
                pool.starmap(download_chunk, download_tasks)

        if func:
            return func(shared_filepath)
