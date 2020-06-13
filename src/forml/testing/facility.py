"""
Testing facility.
"""
import logging
import typing
import uuid

from forml import etl
from forml.conf import provider as provcfg
from forml.etl.dsl import parsing, statement
from forml.etl.dsl.schema import frame, series, kind
from forml.flow import task
from forml.flow.graph import node as nodemod
from forml.flow.graph import view
from forml.flow.pipeline import topology
from forml.runtime import process
from forml.stdlib.operator import simple
from forml.testing import spec

LOGGER = logging.getLogger(__name__)


class TrainSet(etl.Schema):
    """Testing trainset schema.

    The actual fields are irrelevant.
    """
    feature: etl.Field = etl.Field(kind.Integer())
    label: etl.Field = etl.Field(kind.Float())


class TestSet(etl.Schema):
    """Testing testset schema.

    The actual fields are irrelevant.
    """
    feature: etl.Field = etl.Field(kind.Integer())


class Engine(etl.Engine, key='testing'):
    """Special engine to feed the test cases.
    """
    def __init__(self, scenario: spec.Scenario.Input, **kw):
        super().__init__(**kw)
        self._scenario: spec.Scenario.Input = scenario

    @classmethod
    def reader(cls, sources: typing.Mapping[frame.Source, parsing.ResultT], columns: typing.Mapping[
            series.Column, parsing.ResultT]) -> typing.Callable[[statement.Query], typing.Any]:
        def read(query: statement.Query) -> typing.Any:
            """Reader callback.

            Args:
                query: Input query instance.

            Returns: Data.
            """
            try:
                return sources[query.source]
            except KeyError:
                raise RuntimeError(f'Expected either {TrainSet.__schema__.__qualname__} or '
                                   f'{TestSet.__schema__.__qualname__}, got {query}')
        return read

    @property
    def sources(self) -> typing.Mapping[frame.Source, parsing.ResultT]:
        return {
            TrainSet: (self._scenario.train, self._scenario.label),
            TestSet: self._scenario.apply
        }


class Runner:
    """Test runner is a minimal forml pipeline wrapping the tested operator.
    """
    @simple.Labeler.operator
    class Extractor(task.Actor):
        """Just split the features-label pair.
        """
        def apply(self, bundle: typing.Tuple[typing.Any, typing.Any]) -> typing.Tuple[typing.Any, typing.Any]:
            return bundle

    class Initializer(view.Visitor):
        """Visitor that tries to instantiate each node in attempt to validate it.
        """
        def __init__(self):
            self._gids: typing.Set[uuid.UUID] = set()

        def visit_node(self, node: nodemod.Worker) -> None:
            if isinstance(node, nodemod.Worker) and node.gid not in self._gids:
                self._gids.add(node.gid)
                node.spec()

    def __init__(self, params: spec.Scenario.Params, scenario: spec.Scenario.Input, runner: provcfg.Runner):
        self._params: spec.Scenario.Params = params
        self._source: etl.Source = etl.Source.query(train=TrainSet.select(),
                                                    apply=TestSet.select()) >> Runner.Extractor()
        self._engine: etl.Engine = Engine(scenario)
        self._runner: provcfg.Runner = runner

    def __call__(self, operator: typing.Type[topology.Operator]) -> process.Runner:
        instance = operator(*self._params.args, **self._params.kwargs)
        initializer = self.Initializer()
        segment = instance.expand()
        segment.apply.accept(initializer)
        segment.train.accept(initializer)
        segment.label.accept(initializer)
        return self._source.bind(instance).launcher(process.Runner[self._runner.name], self._engine,
                                                    **self._runner.kwargs)
