"""Extract utilities.
"""
import abc
import typing

from forml import error
from forml.etl.dsl import parsing, statement as stmntmod
from forml.etl.dsl.schema import kind as kindmod, series, frame
from forml.flow import task, pipeline
from forml.flow.graph import node, view
from forml.flow.pipeline import topology


class Statement(typing.NamedTuple):
    """Select statement defined as a query and definition of the ordinal expression.
    """
    query: stmntmod.Query
    ordinal: typing.Optional[series.Column]

    class Binding(typing.NamedTuple):
        """Statement bound with particular lower/upper parameters.
        """
        statement: 'Statement'
        lower: typing.Optional[kindmod.Native]
        upper: typing.Optional[kindmod.Native]

        def __call__(self) -> stmntmod.Query:
            """Expand the statement with the provided lower/upper parameters.

            Returns: Expanded query transformed using the associated processor.
            """
            return self.statement(self.lower, self.upper)

    def bind(self, lower: typing.Optional[kindmod.Native] = None,
             upper: typing.Optional[kindmod.Native] = None) -> Binding:
        """Bind the particular lower/upper parameters with this prepared statement.

        Args:
            lower: Optional lower ordinal value.
            upper:  Optional upper ordinal value.

        Returns: statement binding.
        """
        return self.Binding(self, lower, upper)

    def __call__(self, lower: typing.Optional[kindmod.Native] = None,
                 upper: typing.Optional[kindmod.Native] = None) -> stmntmod.Query:
        query = self.query
        if self.ordinal is not None:
            if lower:
                query = query.where(self.ordinal >= lower)
            if upper:
                query = query.where(self.ordinal < upper)
        elif lower or upper:
            raise error.Unexpected('Bounds provided but source not ordinal')
        return query


class Operator(topology.Operator):
    """Basic source operator with optional label extraction.

    Label extractor is expected to be an actor with single input and two output ports - train and actual label.
    """
    def __init__(self, apply: task.Spec, train: typing.Optional[task.Spec] = None,
                 label: typing.Optional[task.Spec] = None):
        self._apply: task.Spec = apply
        self._train: task.Spec = train or apply
        self._label: typing.Optional[task.Spec] = label

    def compose(self, left: topology.Composable) -> pipeline.Segment:
        """Compose the source segment track.

        Returns: Source segment track.
        """
        if not isinstance(left, topology.Origin):
            raise error.Invalid('Source not origin')
        apply: view.Path = view.Path(node.Worker(self._apply, 0, 1))
        train: view.Path = view.Path(node.Worker(self._train, 0, 1))
        label: typing.Optional[view.Path] = None
        if self._label:
            train_tail = node.Future()
            label_tail = node.Future()
            extract = node.Worker(self._label, 1, 2)
            extract[0].subscribe(train.publisher)
            train_tail[0].subscribe(extract[0])
            label_tail[0].subscribe(extract[1])
            train = train.extend(tail=train_tail)
            label = train.extend(tail=label_tail)
        return pipeline.Segment(apply, train, label)


class Reader(metaclass=abc.ABCMeta):
    """Base class for reader implementation.
    """
    class Parser(parsing.Visitor, metaclass=abc.ABCMeta):
        """Parser implementation for this engine.
        """

    def __init__(self, sources: typing.Mapping[frame.Source, parsing.ResultT],
                 columns: typing.Mapping[series.Column, parsing.ResultT]):
        self._sources: typing.Mapping[frame.Source, parsing.ResultT] = sources
        self._columns: typing.Mapping[series.Column, parsing.ResultT] = columns

    def __call__(self, query: stmntmod.Query) -> typing.Any:
        parser = self.Parser(self._sources, self._columns)  # pylint: disable=abstract-class-instantiated
        query.accept(parser)
        return self.read(parser.result)

    @abc.abstractmethod
    def read(self, statement: parsing.ResultT) -> typing.Any:
        """Perform the read operation with the given statement.

        Args:
            statement: Query statement in the reader's native syntax.

        Returns: Data provided by the reader.
        """


class Actor(task.Actor):
    """Data extraction actor using the provided reader and statement to load the data.
    """
    def __init__(self, reader: Reader, statement: Statement.Binding):  # pylint: disable=no-member
        self._reader: Reader = reader
        self._statement: Statement.Binding = statement  # pylint: disable=no-member

    def apply(self) -> typing.Any:
        return self._reader(self._statement())
