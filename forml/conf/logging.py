# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
DEFAULTS = dict(prj_name=conf.PRJNAME, log_facility=handlers.SysLogHandler.LOG_USER, log_path=f'./{conf.PRJNAME}.log')


def setup(*path: pathlib.Path, **defaults: typing.Any):
    """Setup logger according to the params."""
    parser = configparser.ConfigParser(DEFAULTS | defaults)
    tried = set()
    used = parser.read(
        p
        for p in ((b / conf.logcfg).resolve() for b in itertools.chain(conf.PATH, path))
        if not (p in tried or tried.add(p))
    )
    config.fileConfig(parser, disable_existing_loggers=True)
    logging.captureWarnings(capture=True)
    LOGGER.debug('Logging configs: %s', ', '.join(used) or 'none')
    LOGGER.debug('Application configs: %s', ', '.join(str(s) for s in conf.PARSER.sources) or 'none')
    for src, err in conf.PARSER.errors.items():
        LOGGER.warning('Error parsing config %s: %s', src, err)


conf.PARSER.subscribe(setup)  # reload logging config upon main config change to reflect potential new values
