"""
Project tests.
"""
# pylint: disable=no-self-use

import pytest

from forml import project, etl
from forml.etl import expression
from forml.flow import task
from forml.flow.pipeline import topology
from forml.stdlib.operator import simple


class TestBuilder:
    """Builder unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def builder():
        """Builder fixture.
        """
        return project.Descriptor.Builder()

    @staticmethod
    @pytest.fixture(scope='function')
    def pipeline() -> topology.Composable:
        """Pipeline fixture.
        """
        return simple.Consumer(task.Spec('Estimator'))

    @staticmethod
    @pytest.fixture(scope='function')
    def source() -> etl.Source:
        """Source fixture.
        """
        return etl.Source(etl.Extract(expression.Select()))

    @staticmethod
    @pytest.fixture(scope='function')
    def evaluation() -> topology.Composable:
        """Evaluation fixture.
        """
        return simple.Consumer(task.Spec('Estimator'))

    def test_api(self, builder: project.Descriptor.Builder):
        """Testing the builder API.
        """
        assert len(builder) == len(project.Descriptor._fields)
        assert all(f in builder for f in project.Descriptor._fields)

    def test_build(self, builder: project.Descriptor.Builder, source: etl.Source, pipeline: topology.Composable,
                   evaluation: topology.Composable):
        """Testing build.
        """
        with pytest.raises(project.Error):
            builder.build()
        handlers = dict(builder)
        handlers['source'](source)
        handlers['pipeline'](pipeline)
        handlers['evaluation'](evaluation)
        descriptor = builder.build()
        assert descriptor.source == source
        assert descriptor.pipeline == pipeline
        assert descriptor.evaluation == evaluation


@pytest.fixture(scope='session')
def pipeline() -> topology.Composable:
    """Pipeline fixture.
    """
    from project import pipeline
    return pipeline.INSTANCE


@pytest.fixture(scope='session')
def source() -> etl.Source:
    """Source fixture.
    """
    from project import source
    return source.INSTANCE


@pytest.fixture(scope='session')
def evaluation() -> topology.Composable:
    """Pipeline evaluation.
    """
    from project import evaluation
    return evaluation.INSTANCE


class TestDescriptor:
    """Descriptor unit tests.
    """
    def test_invalid(self):
        """Testing with invalid types.
        """
        with pytest.raises(project.Error):
            project.Descriptor('foo', 'bar', 'baz')

    def test_load(self, source: etl.Source, pipeline: topology.Composable, evaluation: topology.Composable):
        """Testing the descriptor loader.
        """
        with pytest.raises(project.Error):
            project.Descriptor.load(foo='bar')
        with pytest.raises(project.Error):
            project.Descriptor.load('foo')
        descriptor = project.Descriptor.load('project')  # project package in this test directory
        assert descriptor.pipeline.__dict__ == pipeline.__dict__
        assert descriptor.source.__dict__ == source.__dict__
        assert descriptor.evaluation.__dict__ == evaluation.__dict__


class TestArtifact:
    """Artifact unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def artifact() -> project.Artifact:
        """Artifact fixture.
        """
        return project.Artifact(package='project')

    def test_descriptor(self, artifact: project.Artifact, source: etl.Source, pipeline: topology.Composable,
                        evaluation: topology.Composable):
        """Testing descriptor access.
        """
        assert artifact.descriptor.pipeline.__dict__ == pipeline.__dict__
        assert artifact.descriptor.source.__dict__ == source.__dict__
        assert artifact.descriptor.evaluation.__dict__ == evaluation.__dict__
