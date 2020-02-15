"""
ForML top level.
"""
import os.path
from logging import handlers

from forml import conf
from forml.conf import logging


__version__ = '0.1.dev0'


class Error(Exception):
    """ForML base exception class.
    """


logging.setup(*(os.path.join(d, conf.LOG_CFGFILE) for d in (conf.SYS_DIR, conf.USR_DIR)),
              prj_name=conf.PRJ_NAME, log_facility=handlers.SysLogHandler.LOG_USER, log_path=f'./{conf.PRJ_NAME}.log')
