"""
ForML top level.
"""
import configparser
import logging
from logging import handlers as loghandlers, config as logconfig
import os.path
import typing

from forml import conf
from forml import flow, etl
from forml.flow import segment


class Error(Exception):
    """ForML base exception class.
    """


def _logsetup(configs: typing.Iterable[str]):
    """Setup logger according to the params.
    """
    defaults = {'log_facility': loghandlers.SysLogHandler.LOG_USER, 'app_name': conf.APP_NAME}
    for cfg_path in configs:
        if os.path.isfile(cfg_path):
            try:
                logconfig.fileConfig(cfg_path, defaults=defaults, disable_existing_loggers=False)
            except configparser.Error as err:
                logging.warning('Unable to read logging config from %s: %s', cfg_path, err)


_logsetup({os.path.join(d, conf.LOG_CFGFILE) for d in (conf.USR_DIR, conf.SYS_DIR)})
logging.debug('Using configs from %s', conf.USED_CONFIGS)
logging.captureWarnings(capture=True)


class Project(typing.Generic[etl.SelectT]):
    """Top level ForML project descriptor.

    """
    def __init__(self):
        self.pipeline: segment.Composable = ...
        self.source: etl.Source[etl.SelectT] = ...
        self.scoring = ...  # cv+metric -> single number
        self.reporting = ...  # arbitrary metrics -> kv list
