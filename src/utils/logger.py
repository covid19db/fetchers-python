import os
import logging


def setup_logger():
    format = '%(asctime)s %(levelname)s %(filename)s:%(lineno)s %(message)s'
    level = os.environ.get("LOGLEVEL", "DEBUG")

    log_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'fetcher.log')

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.WARNING)

    ch = logging.StreamHandler()
    ch.setLevel(level)

    handlers = [fh, logging.StreamHandler()]

    logging.basicConfig(level=level, format=format, handlers=handlers)
