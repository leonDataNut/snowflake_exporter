import logging
import time
import sys

def setup_logging(module=None, level=logging.INFO):  # pragma: no cover
    logger = logging.getLogger(module or '')
    logger.setLevel(level)
    logging.Formatter.converter = time.gmtime
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(processName)s - %(levelname)s - %(message)s'
    )
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def log_msg(log, msg, verbose=True):
    if verbose:
        log.info(msg)