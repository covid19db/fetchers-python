import os
import logging
from utils.config import Config


def setup_logger():
    format = '%(asctime)s %(levelname)s %(name)s %(message)s'
    level = Config.LOGLEVEL
    log_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'fetcher.log')

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.WARNING)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    handlers = [fh, logging.StreamHandler()]
    logging.basicConfig(level=level, format=format, handlers=handlers)
