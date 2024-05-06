"""Logger configuration."""

from datetime import datetime
import logging
import os

from colorlog import ColoredFormatter

logging.getLogger('flake8').propagate = False
logging.getLogger('pydocstyle').propagate = False
log = logging.getLogger()


def config_logging(log_directory: str | None = None, level: int = logging.INFO) -> None:
    """
    Create logging configuration.

    1. Logging to file, if log_directory is specified. The log is more detailed than printed to screen.
        Log file name includes date and time log creation.
    2. Logging to screen.
    3. Tensorflow logs - additional information specify module and function called.
    """
    if log_directory is not None:
        os.makedirs(log_directory, exist_ok=True)
        log_path = os.path.join(log_directory, "log" + datetime.now().strftime("%Y-%m-%d_%H%M") + ".txt")
        log.info(f"Configuring logging to file {log_path}.")
    else:
        log_path = os.devnull

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Configure logging to file
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)-8s %(name)s %(funcName)s   %(message)s',
                        datefmt='%Y-%m-%d,%H:%M:%S',
                        filename=log_path,
                        filemode='w')

    # Add console logger
    console = logging.StreamHandler()
    console.setLevel(level)
    formatter = ColoredFormatter("%(log_color)s%(asctime)s %(levelname)-8s %(message)s",
                                 datefmt='%Y-%m-%d %H:%M:%S',
                                 reset=True,
                                 log_colors={'DEBUG': 'cyan',
                                             'INFO': 'blue',
                                             'WARNING': 'yellow',
                                             'ERROR': 'red',
                                             'CRITICAL': 'red,bg_white'},
                                 secondary_log_colors={},
                                 style='%')
    console.setFormatter(formatter)
    log.addHandler(console)

    # Configure tensorflow logger
    tf_log = logging.getLogger('tensorflow')
    tf_log.setLevel(level)
    tf_formatter = ColoredFormatter("%(black)s%(asctime)s TENSORFLOW    module: %(module)s, function: %(funcName)s",
                                    datefmt='%Y-%m-%d %H:%M:%S',
                                    style='%')
    for h in tf_log.handlers:
        h.setFormatter(tf_formatter)

    log.info(f'Configured logger. Logging level: {logging.getLevelName(level)}')


config_logging()
