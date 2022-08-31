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
Project templating tests.
"""
import pathlib
import types
import typing

import pytest

import forml
from forml import project, setup
from forml.io import asset
from forml.setup import _conf, _templating


def test_find():
    """Template finder tests."""
    assert _templating.find(_conf.CONFIG[_conf.SECTION_TEMPLATING][_conf.OPT_DEFAULT]).exists()
    with pytest.raises(forml.MissingError, match='not found'):
        _templating.find('foobar123')


@pytest.fixture(scope='session')
def project_template() -> pathlib.Path:
    """Fixture for the test project template."""
    return pathlib.Path(__file__).parent / 'template'


@pytest.fixture(scope='session')
def project_requirements() -> typing.Sequence[str]:
    """Fixture for the test project requirements."""
    return 'foo', 'bar', 'baz'


@pytest.fixture(scope='session')
def templating_context(
    project_name: asset.Project.Key,
    project_release: asset.Release.Key,
    project_manifest: project.Manifest,
    project_requirements: typing.Sequence[str],
) -> typing.Mapping[str, typing.Any]:
    """Fixture for the templating context."""
    return types.MappingProxyType(
        {
            'forml': {'version': forml.__version__},
            'project': {
                'name': project_name,
                'package': project_manifest.package,
                'version': project_release,
                'requirements': project_requirements,
            },
        }
    )


def test_generate(
    project_template: pathlib.Path,
    project_name: asset.Project.Key,
    project_release: asset.Release.Key,
    project_manifest: project.Manifest,
    project_requirements: typing.Sequence[str],
    templating_context: typing.Mapping[str, typing.Any],
    tmp_path: pathlib.Path,
):
    """Templating generator test."""
    _templating.generate(tmp_path, project_template, templating_context)
    for modname in ('module', f'{project_manifest.package}.module'):
        module = setup.isolated(modname, tmp_path)
        assert module.FORML_VERSION == forml.__version__
        assert module.PROJECT_NAME == project_name
        assert module.PROJECT_PACKAGE == project_manifest.package
        assert module.PROJECT_VERSION == str(project_release)
        assert module.PROJECT_REQUIREMENTS == list(project_requirements)

    with pytest.raises(forml.InvalidError, match='already exists'):
        _templating.generate(tmp_path, project_template, templating_context)
