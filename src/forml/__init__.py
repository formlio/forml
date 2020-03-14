"""
ForML top level.
"""
import os.path
from logging import handlers

from forml import conf
from forml.conf import logging

__version__ = '0.2.dev0'


logging.setup(*(os.path.join(d, conf.LOGCFG) for d in (conf.SYSDIR, conf.USRDIR)),
              prj_name=conf.PRJNAME, log_facility=handlers.SysLogHandler.LOG_USER, log_path=f'./{conf.PRJNAME}.log')
