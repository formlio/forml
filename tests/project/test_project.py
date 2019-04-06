"""
Project tests.
"""
# pylint: disable=no-self-use

import pytest

from forml import project, etl
from forml.etl import expression
from forml.flow import task, segment
from forml.flow.operator import simple


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
    def pipeline() -> segment.Composable:
        """Pipeline fixture.
        """
        return simple.Consumer(task.Spec('Estimator'))

    @staticmethod
    @pytest.fixture(scope='function')
    def source() -> etl.Source:
        """Source fixture.
        """
        return etl.Source(etl.Extract(expression.Select()))

    def test_api(self, builder: project.Descriptor.Builder):
        """Testing the builder API.
        """
        assert len(builder) == len(project.Descriptor._fields)
        assert all(f in builder for f in project.Descriptor._fields)

    def test_build(self, builder: project.Descriptor.Builder, source: etl.Source, pipeline: segment.Composable):
        """Testing build.
        """
        with pytest.raises(project.Error):
            builder.build()
        handlers = dict(builder)
        handlers['source'](source)
        handlers['pipeline'](pipeline)
        descriptor = builder.build()
        assert descriptor.source == source
        assert descriptor.pipeline == pipeline


class TestDescriptor:
    """Descriptor unit tests.
    """
    def test_invalid(self):
        """Testing with invalid types.
        """
        with pytest.raises(project.Error):
            project.Descriptor('foo', 'bar')

    def test_load(self):
        """Testing the descriptor loader.
        """
        with pytest.raises(project.Error):
            project.Descriptor.load(foo='bar')
        with pytest.raises(project.Error):
            project.Descriptor.load('foo')
        descriptor = project.Descriptor.load('project')  # project package in this test directory
        from project import source, pipeline
        assert descriptor.source is source.INSTANCE
        assert descriptor.pipeline is pipeline.INSTANCE
