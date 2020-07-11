"""
ETL layer.
"""
import collections
import typing

from forml.flow.pipeline import topology
from forml.io.dsl import statement as stmtmod
from forml.io.dsl.schema import series, frame, kind as kindmod
from forml.project import product


class Field(collections.namedtuple('Field', 'kind, name')):
    """Schema field class.
    """
    def __new__(cls, kind: kindmod.Any, name: typing.Optional[str] = None):
        return super().__new__(cls, kind, name)


class Schema(metaclass=frame.Table):  # pylint: disable=invalid-metaclass
    """Base class for table schema definitions. Note the meta class is actually going to turn it into an instance
    of frame.Table.
    """


class Source(typing.NamedTuple):
    """Feed independent data provider description.
    """
    extract: 'Source.Extract'
    transform: typing.Optional[topology.Composable] = None

    class Extract(collections.namedtuple('Extract', 'train, apply, label, ordinal')):
        """Combo of select statements for the different modes.
        """
        def __new__(cls, train: 'stmtmod.Query', apply: 'stmtmod.Query', label: typing.Sequence[series.Column],
                    ordinal: typing.Optional[series.Element]):
            if {c.element for c in train.columns}.intersection(c.element for c in label):
                raise ValueError('Label-feature overlap')
            if ordinal:
                series.Element.ensure(ordinal)
            return super().__new__(cls, train, apply, tuple(label), ordinal)

    @classmethod
    def query(cls, features: 'stmtmod.Query', *label: series.Column, apply: typing.Optional['stmtmod.Query'] = None,
              ordinal: typing.Optional[series.Element] = None) -> 'Source':
        """Create new source with the given extraction.

        Args:
            features: Query defining the train (and possibly apply) features.
            label: List of training label columns.
            apply: Optional query defining the apply features (if different from train ones).
            ordinal: Optional specification of an ordinal column.

        Returns: New source instance.
        """
        return cls(cls.Extract(features, apply or features, label, ordinal))  # pylint: disable=no-member

    def __rshift__(self, transform: topology.Composable) -> 'Source':
        return self.__class__(self.extract, self.transform >> transform if self.transform else transform)

    def bind(self, pipeline: typing.Union[str, topology.Composable], **modules: typing.Any) -> 'product.Artifact':
        """Create an artifact from this source and given pipeline.

        Args:
            pipeline: Pipeline to create the artifact with.
            **modules: Other optional artifact modules.

        Returns: Project artifact instance.
        """
        return product.Artifact(source=self, pipeline=pipeline, **modules)
