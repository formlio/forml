"""
ForML top level.
"""
import os.path
from logging import handlers

from forml import conf
from forml.conf import logging


class Error(Exception):
    """ForML base exception class.
    """


logging.setup(*(os.path.join(d, conf.LOG_CFGFILE) for d in (conf.SYS_DIR, conf.USR_DIR)),
              app_name=conf.APP_NAME, log_facility=handlers.SysLogHandler.LOG_USER, log_path=f'./{conf.APP_NAME}.log')
