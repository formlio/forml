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
Common fixtures.
"""
import configparser
import importlib
import pathlib
import sys
import types
import typing
from unittest import mock

import pytest


@pytest.fixture(scope='session')
def cfg_file() -> pathlib.Path:
    """Fixture for the test config file.
    """
    return pathlib.Path(__file__).parent / 'config.ini'


@pytest.fixture(scope='session')
def conf(cfg_file: pathlib.Path) -> types.ModuleType:
    """Fixture for the forml.conf module.
    """
    class ConfigParser(configparser.ConfigParser):
        """Fake config parser that reads only our config file.
        """
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            super().read([cfg_file])

        def read(self, *_, **__) -> typing.Sequence[str]:  # pylint: disable=signature-differs
            """Ignore actual readings.
            """
            return [str(cfg_file)]

    # pylint: disable=import-outside-toplevel
    with mock.patch('forml.conf.configparser.ConfigParser', return_value=ConfigParser()):
        from forml import conf
        importlib.reload(conf)
    del sys.modules[conf.__name__]
    return conf
