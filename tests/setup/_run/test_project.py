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
ForML cli project group unit tests.
"""
import os.path
import pathlib
import shutil

import pytest
from click import testing

from forml import project as prjmod
from forml.setup._run import project as prjcmd


@pytest.fixture(scope='module')
def path(
    cli_runner: testing.CliRunner, project_package: prjmod.Package, tmp_path_factory: pytest.TempPathFactory
) -> pathlib.Path:
    """Project path fixture."""
    root = tmp_path_factory.mktemp('project')
    cli_runner.invoke(
        prjcmd.group,
        ['--path', str(root), 'init', '--package', project_package.manifest.package, project_package.manifest.name],
    )
    prjpath = root / project_package.manifest.name
    shutil.copytree(project_package.path, prjpath, dirs_exist_ok=True)
    return prjpath


def test_init(cli_runner: testing.CliRunner, tmp_path: pathlib.Path, project_manifest: prjmod.Manifest):
    """Project init test."""
    result = cli_runner.invoke(
        prjcmd.group, ['--path', str(tmp_path), 'init', '--package', project_manifest.package, project_manifest.name]
    )
    assert result.exit_code == 0
    assert os.path.exists(os.path.join(tmp_path, project_manifest.name, *project_manifest.package.split('.')))


def test_test(cli_runner: testing.CliRunner, path: pathlib.Path):
    """Project devqa test."""
    result = cli_runner.invoke(prjcmd.group, ['--path', str(path), 'test'])
    assert result.exit_code == 0
