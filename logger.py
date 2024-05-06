"""Logger configuration."""

import logging

from colorlog import ColoredFormatter

log = logging.getLogger()


def config_logging(level: int = logging.INFO) -> None:
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

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

    log.info(f'Configured logger. Logging level: {logging.getLevelName(level)}')


config_logging()
