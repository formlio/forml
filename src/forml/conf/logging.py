"""
ForML logging.
"""
import configparser
import itertools
import logging
import pathlib
import typing
from logging import config, handlers

from forml import conf

LOGGER = logging.getLogger(__name__)
DEFAULTS = dict(prj_name=conf.PRJNAME,
                log_facility=handlers.SysLogHandler.LOG_USER,
                log_path=f'./{conf.PRJNAME}.log')


def setup(*path: pathlib.Path, **defaults: typing.Any):
    """Setup logger according to the params.
    """
    defaults = {**DEFAULTS, **defaults}
    done = set()
    disable = True
    for candidate in itertools.chain(conf.PATH, path):
        cfg = (candidate / conf.LOGCFG).resolve()
        if cfg in done:
            continue
        done.add(cfg)
        if cfg.is_file():
            try:
                config.fileConfig(cfg, defaults=defaults, disable_existing_loggers=disable)
            except configparser.Error as err:
                logging.warning('Unable to read logging config from %s: %s', cfg, err)
                continue
            else:
                disable = False

    logging.captureWarnings(capture=True)
    LOGGER.debug('Using configs: %s', ', '.join(conf.SRC) or 'none')
