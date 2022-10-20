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
ForML setup.
"""
import sys

from ._conf import APPNAME, CONFIG, PRJNAME, SYSDIR, USRDIR, tmpdir
from ._importer import Finder, context, isolated, load, search
from ._logging import LOGGING, logging
from ._provider import Feed, Gateway, Inventory, Provider, Registry, Runner, Sink
from ._run import cli

__all__ = [
    'APPNAME',
    'cli',
    'CONFIG',
    'context',
    'Feed',
    'Finder',
    'Gateway',
    'Inventory',
    'isolated',
    'load',
    'logging',
    'LOGGING',
    'PRJNAME',
    'Provider',
    'Registry',
    'Runner',
    'search',
    'Sink',
    'SYSDIR',
    'tmpdir',
    'USRDIR',
]


for _path in (USRDIR, SYSDIR):
    if _path not in sys.path:
        sys.path.append(str(_path))
