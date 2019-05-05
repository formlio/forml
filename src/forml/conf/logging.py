"""
ForML logging.
"""
import configparser
import os.path
import typing
import logging
from logging import config

from forml import conf

LOGGER = logging.getLogger(__name__)
VENDOR = os.path.join(os.path.dirname(__file__), 'logging.ini')


def setup(*configs: str, **defaults: typing.Any):
    """Setup logger according to the params.
    """
    config.fileConfig(VENDOR, defaults=defaults, disable_existing_loggers=True)
    for cfg in configs:
        if os.path.isfile(cfg):
            try:
                config.fileConfig(cfg, defaults=defaults, disable_existing_loggers=False)
            except configparser.Error as err:
                logging.warning('Unable to read logging config from %s: %s', cfg, err)

    logging.captureWarnings(capture=True)
    LOGGER.info('Using configs from %s', conf.USED_CONFIGS)
