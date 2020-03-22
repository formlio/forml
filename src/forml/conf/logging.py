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
    names = conf.LOGCFG, conf.get(conf.OPT_LOGCFG)
    todo = ((b / n).resolve() for b, n in itertools.product(itertools.chain(conf.PATH, path), names))
    tried = set()
    used = parser.read((p for p in todo if not (p in tried or tried.add(p))))
    config.fileConfig(parser, disable_existing_loggers=True)
    logging.captureWarnings(capture=True)
    LOGGER.debug('Application configs: %s', ', '.join(conf.SRC) or 'none')
    LOGGER.debug('Logging configs: %s', ', '.join(used) or 'none')
