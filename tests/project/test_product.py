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
Project tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml import error
from forml.flow.pipeline import topology
from forml.project import product, distribution, importer, component as compmod
from forml.lib.flow.operator.generic import simple


class TestBuilder:
    """Builder unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def builder():
        """Builder fixture."""
        return product.Descriptor.Builder()

    @staticmethod
    @pytest.fixture(scope='function')
    def pipeline(spec) -> topology.Composable:
        """Pipeline fixture."""
        return simple.Consumer(spec)

    @staticmethod
    @pytest.fixture(scope='function')
    def source() -> compmod.Source:
        """Source fixture."""
        return compmod.Source(None)

    @staticmethod
    @pytest.fixture(scope='function')
    def evaluation() -> compmod.Evaluation:
        """Evaluation fixture."""
        return compmod.Evaluation(None, None)

    def test_api(self, builder: product.Descriptor.Builder):
        """Testing the builder API."""
        assert len(builder) == len(product.Descriptor._fields)
        assert all(f in builder for f in product.Descriptor._fields)

    def test_build(
        self,
        builder: product.Descriptor.Builder,
        source: compmod.Source,
        pipeline: topology.Composable,
        evaluation: compmod.Evaluation,
    ):
        """Testing build."""
        with pytest.raises(error.Invalid):
            builder.build()
        handlers = dict(builder)
        handlers['source'](source)
        handlers['pipeline'](pipeline)
        handlers['evaluation'](evaluation)
        descriptor = builder.build()
        assert descriptor.source == source
        assert descriptor.pipeline == pipeline
        assert descriptor.evaluation == evaluation


def load(package: distribution.Package, component: str) -> typing.Any:
    """Helper for importing the project component module."""
    module = f'{package.manifest.package}.{package.manifest.modules.get(component, component)}'
    return importer.isolated(module, package.path).INSTANCE


@pytest.fixture(scope='session')
def pipeline(project_package: distribution.Package) -> topology.Composable:
    """Pipeline fixture."""
    return load(project_package, 'pipeline')


@pytest.fixture(scope='session')
def source(project_package: distribution.Package) -> topology.Composable:
    """Source fixture."""
    return load(project_package, 'source')


@pytest.fixture(scope='session')
def evaluation(project_package: distribution.Package) -> compmod.Evaluation:
    """Evaluation fixture."""
    return load(project_package, 'evaluation')


class TestDescriptor:
    """Descriptor unit tests."""

    def test_invalid(self):
        """Testing with invalid types."""
        with pytest.raises(error.Invalid):
            product.Descriptor('foo', 'bar', 'baz')

    def test_load(
        self,
        project_package: distribution.Package,
        source: compmod.Source,
        pipeline: topology.Composable,
        evaluation: compmod.Evaluation,
    ):
        """Testing the descriptor loader."""
        with pytest.raises(error.Unexpected):
            product.Descriptor.load(foo='bar')
        with pytest.raises(error.Invalid):
            product.Descriptor.load('foo')
        descriptor = product.Descriptor.load(project_package.manifest.package, project_package.path)
        assert repr(descriptor.pipeline) == repr(pipeline)
        assert repr(descriptor.source) == repr(source)
        assert repr(descriptor.evaluation) == repr(evaluation)


class TestArtifact:
    """Artifact unit tests."""

    def test_descriptor(
        self, project_artifact, source: compmod.Source, pipeline: topology.Composable, evaluation: compmod.Evaluation
    ):
        """Testing descriptor access."""
        assert repr(project_artifact.descriptor.pipeline) == repr(pipeline)
        assert repr(project_artifact.descriptor.source) == repr(source)
        assert repr(project_artifact.descriptor.evaluation) == repr(evaluation)

    def test_launcher(self, project_artifact: product.Artifact):
        """Testing launcher access."""
        assert project_artifact.launcher
