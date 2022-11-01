"""Download S3 files concurrently.

Usage:
    s3pd [options] <SOURCE> [<DESTINATION>]

Options:
    -p,--processes=<PROCESSES>      Number of concurrent download processes
                                    [default: 4]
    -c,--chunksize=<CHUNKSIZE>      Size of chunks for each worker, in bytes
                                    [default: 8388608]
    -u,--unsigned                   Use unsigned requests
    --start-method=<START_METHOD>   Override OS specific worker processes start method,
                                    e.g. fork, spawn or forkserver
"""
from docopt import docopt
from multiprocessing import get_context

from s3pd import s3pd, version

def main():
    args = docopt(__doc__, version=version.__version__)

    source = args['<SOURCE>']
    destination = args['<DESTINATION>']

    processes = int(args['--processes'])
    chunksize = int(args['--chunksize'])
    signed = not args['--unsigned']

    start_method = args['--start-method']
    assert start_method is None or start_method in ('fork', 'spawn', 'forkserver')
    multiprocessing_context = get_context(start_method)

    s3pd(
        url=source,
        processes=processes,
        chunksize=chunksize,
        destination=destination,
        signed=signed,
        mp_context=multiprocessing_context,
    )
