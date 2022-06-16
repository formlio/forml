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
import pathlib

import pytest
from click import testing

from forml import cli, conf


@pytest.fixture(scope='session')
def cfg_file() -> pathlib.Path:
    """Fixture for the test config file."""
    return pathlib.Path(__file__).parent / conf.APPCFG


def test_main(cfg_file: pathlib.Path):
    """Basic cli test."""
    runner = testing.CliRunner()
    result = runner.invoke(cli.group, ['--config', str(cfg_file), 'project'])
    assert result.exit_code == 0
