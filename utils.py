import os
import errno
import logging

from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta

def check_and_create_file(file_path):
    if not os.path.exists(os.path.dirname(file_path)):
        try:
            os.makedirs(os.path.dirname(file_path))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    if os.path.isfile(file_path) is not True:
        f = open(file_path, "w")


def with_in_time(timestamp, days):
    """Filter time with in number's day

    Args:
        timestamp (int): timestamp of date
        day (int): How many days to filter

    Returns: True/False
    """
    data_time = datetime.utcfromtimestamp(timestamp)
    if datetime.utcnow() - data_time < timedelta(days=days):
        return True


def create_logger(log_name, log_folder, show_logs):
    """Handles the creation and retrieval of loggers to avoid
        re-instantiation.

    Args:
        log_name (str): logger name
        log_folder ([type]):  folder of log
        show_logs (bool): True or False
        log_handler ([type], optional): [description]. Defaults to None.

    Returns:
        [type]: [description]
    """
    # initialize and setup logging system for the InstaPy object
    logger = logging.getLogger(log_name)
    if (logger.hasHandlers()):
        logger.handlers.clear()

    logger.setLevel(logging.DEBUG)
    # log name and format
    general_log = f"{log_folder}{log_name}.log"
    check_and_create_file(general_log)

    file_handler = logging.FileHandler(general_log)
    # log rotation, 5 logs with 10MB size each one
    file_handler = RotatingFileHandler(
        general_log, maxBytes=10 * 1024 * 1024, backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    extra = {"Spider": log_name}
    logger_formatter = logging.Formatter(
        "%(levelname)s [%(asctime)s] [%(Spider)s]  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(logger_formatter)
    logger.addHandler(file_handler)
    # otherwise root logger prints things again
    logger.propagate = False

    if show_logs is True:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logger_formatter)
        logger.addHandler(console_handler)

    logger = logging.LoggerAdapter(logger, extra)
    return logger