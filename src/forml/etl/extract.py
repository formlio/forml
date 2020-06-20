"""Extract utilities.
"""
import abc
import typing

from forml import error
from forml.etl.dsl import parsing, statement as stmtmod
from forml.etl.dsl.schema import kind as kindmod, series, frame
from forml.flow import task, pipeline
from forml.flow.graph import node, view
from forml.flow.pipeline import topology


class Statement(typing.NamedTuple):
    """Select statement defined as a query and definition of the ordinal expression.
    """
    prepared: 'Prepared'
    lower: typing.Optional[kindmod.Native]
    upper: typing.Optional[kindmod.Native]

    class Prepared(typing.NamedTuple):
        """Statement bound with particular lower/upper parameters.
        """
        query: stmtmod.Query
        ordinal: typing.Optional[series.Element]

        def __call__(self, lower: typing.Optional[kindmod.Native] = None,
                     upper: typing.Optional[kindmod.Native] = None) -> stmtmod.Query:
            query = self.query
            if self.ordinal is not None:
                if lower:
                    query = query.where(self.ordinal >= lower)
                if upper:
                    query = query.where(self.ordinal < upper)
            elif lower or upper:
                raise error.Unexpected('Bounds provided but source not ordinal')
            return query

    @classmethod
    def prepare(cls, query: stmtmod.Query, ordinal: typing.Optional[series.Element],
                lower: typing.Optional[kindmod.Native] = None,
                upper: typing.Optional[kindmod.Native] = None) -> 'Statement':
        """Bind the particular lower/upper parameters with this prepared statement.

        Args:
            query: Base statement query.
            ordinal: Optional ordinal column specification.
            lower: Optional lower ordinal value.
            upper:  Optional upper ordinal value.

        Returns: prepared statement binding.
        """
        return cls(cls.Prepared(query, ordinal), lower, upper)  # pylint: disable=no-member

    def __call__(self) -> stmtmod.Query:
        """Expand the statement with the provided lower/upper parameters.

        Returns: Expanded query transformed using the associated processor.
        """
        return self.prepared(self.lower, self.upper)


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
    class Actor(task.Actor):
        """Data extraction actor using the provided reader and statement to load the data.
        """
        def __init__(self, reader: typing.Callable[[stmtmod.Query], typing.Any], statement: Statement):
            self._reader: typing.Callable[[stmtmod.Query], typing.Any] = reader
            self._statement: Statement = statement

        def apply(self) -> typing.Any:
            return self._reader(self._statement())

    def __init__(self, sources: typing.Mapping[frame.Source, parsing.ResultT],
                 columns: typing.Mapping[series.Column, parsing.ResultT]):
        self._sources: typing.Mapping[frame.Source, parsing.ResultT] = sources
        self._columns: typing.Mapping[series.Column, parsing.ResultT] = columns

    def __call__(self, query: stmtmod.Query) -> typing.Any:
        parser = self.parser(self._sources, self._columns)
        query.accept(parser)
        return self.read(parser.result)

    @classmethod
    @abc.abstractmethod
    def parser(cls, sources: typing.Mapping[frame.Source, parsing.ResultT],
               columns: typing.Mapping[series.Column, parsing.ResultT]) -> parsing.Statement:
        """Return the parser instance of this reader.

        Args:
            sources: Source mappings to be used by the parser.
            columns: Column mappings to be used by the parser.

        Returns: Parser instance.
        """

    @classmethod
    @abc.abstractmethod
    def read(cls, statement: parsing.ResultT) -> typing.Any:
        """Perform the read operation with the given statement.

        Args:
            statement: Query statement in the reader's native syntax.

        Returns: Data provided by the reader.
        """


class Selector(metaclass=abc.ABCMeta):
    """Base class for selector implementation.
    """
    class Actor(task.Actor):
        """Data extraction actor using the provided reader and statement to load the data.
        """
        def __init__(self, selector: typing.Callable[[typing.Any, typing.Sequence[series.Column]], typing.Any],
                     features: typing.Sequence[series.Column], label: typing.Sequence[series.Column]):
            self._selector: typing.Callable[[typing.Any, typing.Sequence[series.Column]], typing.Any] = selector
            self._features: typing.Sequence[series.Column] = features
            self._label: typing.Sequence[series.Column] = label

        def apply(self, features: typing.Any) -> typing.Tuple[typing.Any, typing.Any]:
            return self._selector(features, self._features), self._selector(features, self._label)

    def __init__(self, columns: typing.Mapping[series.Column, parsing.ResultT]):
        self._columns: typing.Mapping[series.Column, parsing.ResultT] = columns

    def __call__(self, source: typing.Any, selection: typing.Sequence[series.Column]) -> typing.Any:
        def parse(column: series.Column) -> parsing.ResultT:
            parser = self.parser(self._columns)
            column.accept(parser)
            return parser.result

        return self.select(source, [parse(c) for c in selection])

    @classmethod
    @abc.abstractmethod
    def parser(cls, columns: typing.Mapping[series.Column, parsing.ResultT]) -> parsing.Series:
        """Return the parser instance of this selector.

        Args:
            columns: Column mappings to be used by the parser.

        Returns: Parser instance.
        """

    @classmethod
    @abc.abstractmethod
    def select(cls, source: typing.Any, subset: typing.Sequence[parsing.ResultT]) -> typing.Any:
        """Perform the select operation with the given list of columns.

        Args:
            source: Input dataset to select from.
            subset: List of columns in the reader's native syntax.

        Returns: Data provided by the reader.
        """
