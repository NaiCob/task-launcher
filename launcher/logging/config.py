# Configure logging
import typing

import datetime
import logging
import logging.handlers
import os


def init_logging():
    """Initialize the logging (handlers and formating)."""
    filename = os.environ.get("LOG_FILENAME")
    handlers: typing.List[typing.Any] = [logging.StreamHandler()]
    if filename:
        handlers.append(
            logging.handlers.TimedRotatingFileHandler(
                filename, when="midnight", atTime=datetime.time(20)
            )
        )

    formater = logging.Formatter(
        fmt="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", datefmt="%y-%m-%d %H:%M:%S"
    )

    for handler in handlers:
        handler.setFormatter(formater)

    levels = {
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
    }
    level = levels.get(os.environ.get("LOG_LEVEL", ""), logging.INFO)

    for handler in handlers:
        handler.setLevel(level)

    rootLogger = logging.getLogger()
    for handler in handlers:
        rootLogger.addHandler(handler)
    rootLogger.setLevel(level)

    if os.environ.get("LOG_LEVEL") not in levels:
        logging.warning("Log level was not specified and it defaulted to INFO")

    # set the log level of py4j to info, debug level is too verbose
    logging.getLogger("py4j").setLevel(logging.ERROR)
