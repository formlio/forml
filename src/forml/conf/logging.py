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
    parser = configparser.ConfigParser({**DEFAULTS, **defaults})
    used = list()
    for base, name in itertools.product(itertools.chain(conf.PATH, path), [conf.LOGCFG, conf.get(conf.OPT_LOGCFG)]):
        cfg = (base / name).resolve()
        if cfg in used or not cfg.is_file():
            continue
        try:
            parser.read(cfg)
        except configparser.Error as err:
            logging.warning('Unable to read logging config from %s: %s', cfg, err)
            continue
        used.append(cfg)
    config.fileConfig(parser, disable_existing_loggers=True)
    logging.captureWarnings(capture=True)
    LOGGER.debug('Application configs: %s', ', '.join(conf.SRC) or 'none')
    LOGGER.debug('Logging configs: %s', ', '.join(str(p) for p in used) or 'none')
