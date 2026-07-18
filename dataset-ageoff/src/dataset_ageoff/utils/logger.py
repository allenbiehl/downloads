# pylint: disable=missing-module-docstring
import logging

loggers = {}


def get_logger(name: str = None):
    """
    Get custom logger instance
    """
    if name in loggers:
        return loggers[name]

    return create_logger(name)


def create_logger(name: str):
    """
    Created named logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    loggers[name] = logger
    return logger


root_logger = get_logger()
