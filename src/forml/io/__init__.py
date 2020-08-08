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
ETL layer.
"""
import abc
import typing

from forml import provider
from forml.conf import provider as provcfg
from forml.flow import task, pipeline
from forml.flow.pipeline import topology
from forml.io.etl import extract

if typing.TYPE_CHECKING:
    from forml.io import etl as etlmod
    from forml.io.dsl import parsing, statement as stmtmod
    from forml.io.dsl.schema import series, frame, kind as kindmod


class Feed(provider.Interface, default=provcfg.Feed.default):
    """ETL feed is the implementation of a specific datasource access layer.
    """
    def __init__(self, **readerkw):
        self._readerkw: typing.Dict[str, typing.Any] = readerkw

    def load(self, source: 'etlmod.Source', lower: typing.Optional['kindmod.Native'] = None,
             upper: typing.Optional['kindmod.Native'] = None) -> pipeline.Segment:
        """Provide a flow track implementing the etl actions.

        Args:
            source: Independent datasource descriptor.
            lower: Optional ordinal lower bound.
            upper: Optional ordinal upper bound.

        Returns: Flow track.
        """
        def reader(query: 'stmtmod.Query') -> task.Spec:
            """Helper for creating the reader actor spec for given query.

            Args:
                query: Data loading statement.

            Returns: Reader actor spec.
            """
            return extract.Reader.Actor.spec(self.reader(self.sources, self.columns, **self._readerkw),
                                             extract.Statement.prepare(query, source.extract.ordinal, lower, upper))

        train: 'stmtmod.Query' = source.extract.train
        label: typing.Optional[task.Spec] = None
        if source.extract.label:
            train = train.select(*(*source.extract.train.columns, *source.extract.label))
            label = extract.Slicer.Actor.spec(self.slicer(train.columns, self.columns), source.extract.train.columns,
                                              source.extract.label)
        loader: topology.Composable = extract.Operator(reader(source.extract.apply), reader(train), label)
        if source.transform:
            loader >>= source.transform
        return loader.expand()

    @classmethod
    @abc.abstractmethod
    def reader(cls, sources: typing.Mapping['frame.Source', 'parsing.ResultT'],
               columns: typing.Mapping['series.Column', 'parsing.ResultT'],
               **kwargs: typing.Any) -> typing.Callable[['stmtmod.Query'], extract.Columnar]:
        """Return the reader instance of this feed (any callable, presumably extract.Reader).

        Args:
            sources: Source mappings to be used by the reader.
            columns: Column mappings to be used by the reader.
            kwargs: Optional reader keyword arguments.

        Returns: Reader instance.
        """

    @classmethod
    def slicer(cls, schema: typing.Sequence['series.Column'],
               columns: typing.Mapping['series.Column', 'parsing.ResultT']) -> typing.Callable[
                   [extract.Columnar, typing.Union[slice, int]], extract.Columnar]:
        """Return the slicer instance of this feed, that is able to split the loaded dataset column-wise.

        This default slicer is plain positional sequence slicer.

        Args:
            schema: List of expected columns to be sliced from.
            columns: Column mappings to be used by the selector.

        Returns: Slicer instance.
        """
        return extract.Slicer(schema, columns)

    @property
    def sources(self) -> typing.Mapping['frame.Source', 'parsing.ResultT']:
        """The explicit sources mapping implemented by this feed to be used by the query parser.

        Returns: Sources mapping.
        """
        return {}

    @property
    def columns(self) -> typing.Mapping['series.Column', 'parsing.ResultT']:
        """The explicit columns mapping implemented by this feed to be used by the query parser.

        Returns: Columns mapping.
        """
        return {}
