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
ForML config unit tests.
"""
# pylint: disable=protected-access,no-self-use
import pathlib
import types


def test_exists(cfg_file: pathlib.Path):
    """Test the config file exists.
    """
    assert cfg_file.is_file()


def test_src(conf: types.ModuleType, cfg_file: pathlib.Path):
    """Test the registry config field.
    """
    assert set(conf.SRC) == {str(cfg_file)}


def test_get(conf: types.ModuleType):
    """Test the get value matches the test config.ini
    """
    assert conf.get('foobar') == 'baz'
