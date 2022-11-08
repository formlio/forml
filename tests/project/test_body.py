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

import pytest

import forml
from forml import flow, io, project
from forml.io import layout
from forml.pipeline import wrap


class TestBuilder:
    """Builder unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def builder():
        """Builder fixture."""
        return project.Components.Builder()

    @staticmethod
    @pytest.fixture(scope='function')
    def pipeline(
        actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]]
    ) -> flow.Composable:
        """Pipeline fixture."""
        return wrap.Operator.apply(actor_builder.actor, *actor_builder.args, **actor_builder.kwargs)()

    @staticmethod
    @pytest.fixture(scope='function')
    def source() -> project.Source:
        """Source fixture."""
        return project.Source(None)

    @staticmethod
    @pytest.fixture(scope='function')
    def evaluation() -> project.Evaluation:
        """Evaluation fixture."""
        return project.Evaluation(None, None)

    def test_api(self, builder: project.Components.Builder):
        """Testing the builder API."""
        assert len(builder) == len(project.Components._fields)
        assert all(f in builder for f in project.Components._fields)

    def test_build(
        self,
        builder: project.Components.Builder,
        source: project.Source,
        pipeline: flow.Composable,
        evaluation: project.Evaluation,
    ):
        """Testing build."""
        with pytest.raises(forml.InvalidError):
            builder.build()
        handlers = dict(builder)
        handlers['source'](source)
        handlers['pipeline'](pipeline)
        handlers['evaluation'](evaluation)
        components = builder.build()
        assert components.source == source
        assert components.pipeline == pipeline
        assert components.evaluation == evaluation


class TestDescriptor:
    """Descriptor unit tests."""

    def test_invalid(self):
        """Testing with invalid types."""
        with pytest.raises(forml.InvalidError):
            project.Components('foo', 'bar', 'baz')

    def test_load(
        self,
        project_package: project.Package,
        project_components: project.Components,
    ):
        """Testing the components loader."""
        with pytest.raises(forml.UnexpectedError):
            project.Components.load(foo='bar')
        with pytest.raises(forml.InvalidError):
            project.Components.load('foo')
        components = project.Components.load(project_package.manifest.package, project_package.path)
        assert repr(components.pipeline) == repr(project_components.pipeline)
        assert repr(components.source) == repr(project_components.source)
        assert repr(components.evaluation) == repr(project_components.evaluation)


class TestArtifact:
    """Artifact unit tests."""

    def test_components(self, project_artifact: project.Artifact, project_components: project.Components):
        """Testing descriptor access."""
        assert repr(project_artifact.components.pipeline) == repr(project_components.pipeline)
        assert repr(project_artifact.components.source) == repr(project_components.source)
        assert repr(project_artifact.components.evaluation) == repr(project_components.evaluation)

    def test_launcher(self, project_artifact: project.Artifact, feed_instance: io.Feed):
        """Testing launcher access."""
        assert project_artifact.launcher('dask', [feed_instance]).eval()
