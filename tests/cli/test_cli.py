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
ForML cli unit tests.
"""
# pylint: disable=no-self-use
import argparse
import importlib
import pathlib
import sys
from unittest import mock

import pytest


@pytest.fixture(scope='session')
def cfg_file() -> pathlib.Path:
    """Fixture for the test config file.
    """
    return pathlib.Path(__file__).parent / 'config.ini'


def test_parse(cfg_file: pathlib.Path):
    """Fixture for the forml.conf module.
    """
    # pylint: disable=import-outside-toplevel
    with mock.patch('forml.cli.argparse.ArgumentParser.parse_known_args',
                    return_value=(argparse.Namespace(config=cfg_file.open('r')), [])):
        from forml import cli
        importlib.reload(cli)

    del sys.modules[cli.__name__]
    from forml import conf
    assert str(cfg_file) in conf.SRC
