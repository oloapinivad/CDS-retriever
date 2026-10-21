"""Command-line entry point: parallel retrieval and postprocessing of ERA5 data"""

import logging
import sys

from .config import apply_args, load_config, parser, print_config, validate_config
from .runner import LOG_FORMAT, RetrievalError, retrieve

log = logging.getLogger('cds_retriever')


def setup_logging(level='INFO'):
    """Configure the root logger (command line only: the library never touches it)"""
    logging.basicConfig(level=level.upper(), format=LOG_FORMAT, force=True)


def main(argv=None):
    """Main entry point"""
    args = parser(argv)
    setup_logging(args.loglevel)

    try:
        config = validate_config(apply_args(load_config(args.config, validate=False), args))
    except (OSError, ValueError) as exc:
        log.error('%s', exc)
        return 1
    print_config(config)

    try:
        retrieve(config, dry_run=args.dry_run)
    except RetrievalError as exc:
        log.error('Completed with %d failures: %s', len(exc.failures), ', '.join(exc.failures))
        return 1
    except Exception as exc:
        log.error('%s', exc)
        return 1
    log.info('All done!')
    return 0


if __name__ == '__main__':
    sys.exit(main())
