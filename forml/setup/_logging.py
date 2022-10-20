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
import logging as logmod
import pathlib
from logging import config

from . import _conf

LOGGER = logmod.getLogger(__name__)

#: The logging parser instance with all the current configuration
LOGGING = configparser.ConfigParser(_conf.CONFIG[_conf.SECTION_LOGGING])


def logging(*path: pathlib.Path):
    """Setup logger according to the params."""
    tried = set()
    used = LOGGING.read(
        p
        for p in (
            (b / _conf.CONFIG[_conf.SECTION_LOGGING][_conf.OPT_CONFIG]).resolve()
            for b in itertools.chain(_conf.PATH, path)
        )
        if not (p in tried or tried.add(p))
    )
    config.fileConfig(LOGGING, disable_existing_loggers=False)
    logmod.captureWarnings(capture=True)
    LOGGER.debug('Logging configs: %s', ', '.join(used) or 'none')
    LOGGER.debug('Application configs: %s', ', '.join(str(s) for s in _conf.CONFIG.sources) or 'none')
    for src, err in _conf.CONFIG.errors.items():
        LOGGER.warning('Error parsing config %s: %s', src, err)


_conf.CONFIG.subscribe(logging)  # reload logging config upon main config change to reflect potential new values
