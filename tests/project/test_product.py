"""
Project tests.
"""
# pylint: disable=no-self-use

import pytest

from forml import etl, error
from forml.flow.pipeline import topology
from forml.project import product, distribution, importer
from forml.stdlib.operator import simple


class TestBuilder:
    """Builder unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def builder():
        """Builder fixture.
        """
        return product.Descriptor.Builder()

    @staticmethod
    @pytest.fixture(scope='function')
    def pipeline(spec) -> topology.Composable:
        """Pipeline fixture.
        """
        return simple.Consumer(spec)

    @staticmethod
    @pytest.fixture(scope='function')
    def source() -> etl.Source:
        """Source fixture.
        """
        return etl.Source(etl.Extract(etl.Select()))

    @staticmethod
    @pytest.fixture(scope='function')
    def evaluation(spec) -> topology.Composable:
        """Evaluation fixture.
        """
        return simple.Consumer(spec)

    def test_api(self, builder: product.Descriptor.Builder):
        """Testing the builder API.
        """
        assert len(builder) == len(product.Descriptor._fields)
        assert all(f in builder for f in product.Descriptor._fields)

    def test_build(self, builder: product.Descriptor.Builder, source: etl.Source, pipeline: topology.Composable,
                   evaluation: topology.Composable):
        """Testing build.
        """
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


def load(package: distribution.Package, component: str) -> topology.Composable:
    """Helper for importing the project component module.
    """
    module = f'{package.manifest.package}.{package.manifest.modules.get(component, component)}'
    return importer.isolated(module, package.path).INSTANCE


@pytest.fixture(scope='session')
def pipeline(project_package: distribution.Package) -> topology.Composable:
    """Pipeline fixture.
    """
    return load(project_package, 'pipeline')


@pytest.fixture(scope='session')
def source(project_package: distribution.Package) -> topology.Composable:
    """Source fixture.
    """
    return load(project_package, 'source')


@pytest.fixture(scope='session')
def evaluation(project_package: distribution.Package) -> topology.Composable:
    """Evaluation fixture.
    """
    return load(project_package, 'evaluation')


class TestDescriptor:
    """Descriptor unit tests.
    """
    def test_invalid(self):
        """Testing with invalid types.
        """
        with pytest.raises(error.Invalid):
            product.Descriptor('foo', 'bar', 'baz')

    def test_load(self, project_package: distribution.Package,
                  source: etl.Source, pipeline: topology.Composable, evaluation: topology.Composable):
        """Testing the descriptor loader.
        """
        with pytest.raises(error.Unexpected):
            product.Descriptor.load(foo='bar')
        with pytest.raises(error.Invalid):
            product.Descriptor.load('foo')
        descriptor = product.Descriptor.load(project_package.manifest.package, project_package.path)
        assert descriptor.pipeline.__dict__ == pipeline.__dict__
        assert descriptor.source.__dict__ == source.__dict__
        assert descriptor.evaluation.__dict__ == evaluation.__dict__


class TestArtifact:
    """Artifact unit tests.
    """
    def test_descriptor(self, project_artifact, source: etl.Source, pipeline: topology.Composable,
                        evaluation: topology.Composable):
        """Testing descriptor access.
        """
        assert project_artifact.descriptor.pipeline.__dict__ == pipeline.__dict__
        assert project_artifact.descriptor.source.__dict__ == source.__dict__
        assert project_artifact.descriptor.evaluation.__dict__ == evaluation.__dict__

    def test_launcher(self, project_artifact: product.Artifact):
        """Testing launcher access.
        """
        assert project_artifact.launcher
