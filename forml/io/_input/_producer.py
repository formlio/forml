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
Producer implementation.
"""
import abc
import functools
import itertools
import logging
import typing

import forml
from forml import flow
from forml.io import dsl, layout
from forml.io.dsl import parser as parsmod

LOGGER = logging.getLogger(__name__)


class Reader(typing.Generic[parsmod.Source, parsmod.Feature, layout.Native], metaclass=abc.ABCMeta):
    """Base class for producer implementation."""

    def __init__(
        self,
        sources: typing.Mapping[dsl.Source, parsmod.Source],
        features: typing.Mapping[dsl.Feature, parsmod.Feature],
        **kwargs: typing.Any,
    ):
        self._sources: typing.Mapping[dsl.Source, parsmod.Source] = sources
        self._features: typing.Mapping[dsl.Feature, parsmod.Feature] = features
        self._kwargs: typing.Mapping[str, typing.Any] = kwargs

    def __repr__(self):
        return flow.name(self.__class__, **self._kwargs)

    def __call__(self, query: dsl.Query, entry: typing.Optional[layout.Entry] = None) -> layout.Tabular:
        """Reader entrypoint.

        It operates in two possible modes:
            * extraction - when launched just using the query without the `entry` parameter, it simply executes
              the query against the backend.
            * augmentation - if `entry` is provided, it is interpreted as the actual source to be returned but
              potentially incomplete in terms of the expected schema; in which case the reader is supposed to just
              augment the partial data to meet the query schema.

        Args:
            query: The query DSL specifying the extracted data.
            entry: Optional - potentially incomplete - labelled literal columns to be augmented according
                     to the query.

        Returns:
            Data extracted according to the query.
        """
        if entry:
            complete, indices = self._match_entry(query.schema, entry.schema)
            if not complete:
                # here we would go into augmentation mode - when implemented
                raise forml.InvalidError('Augmentation not yet supported')
            return entry.data.take_columns(indices) if indices else entry.data

        LOGGER.debug('Parsing ETL query')
        with self.parser(self._sources, self._features) as visitor:
            query.accept(visitor)
            result = visitor.fetch()
        LOGGER.debug('Starting ETL read using: %s', result)
        return self.format(query.schema, self.read(result, **self._kwargs))

    @functools.cache
    def _match_entry(  # pylint: disable=no-self-use
        self, query: dsl.Source.Schema, entry: dsl.Source.Schema
    ) -> tuple[bool, typing.Optional[typing.Sequence[int]]]:
        """Match the entry schema against the query.

        Return True if the entry is a proper subset of the query (containing all fields of the query) in which case
        also return the indices matching the ordering of the query.

        Args:
            query: Schema of the master ETL query.
            entry: Schema of the explicitly provided (partial) input data.

        Returns:
            Tuple of a boolean indicating a proper entry subset and its position-wise indexing in relation to the
            query if not identical.
        """

        def names(schema: dsl.Source.Schema) -> typing.Iterable[str]:
            """Extract the schema field names."""
            return (f.name for f in schema)

        query_names = tuple(names(query))
        source = {}
        identical = True
        for index, (demand, supply) in enumerate(itertools.zip_longest(query_names, names(entry))):
            source[supply] = index
            if not supply and demand not in source:
                return False, None
            if identical and supply != demand:
                identical = False
        if identical:
            return True, None
        indices = []
        for column in query_names:
            if column not in source:
                return False, None
            indices.append(source[column])
        return True, tuple(indices)

    @classmethod
    @abc.abstractmethod
    def parser(
        cls,
        sources: typing.Mapping[dsl.Source, parsmod.Source],
        features: typing.Mapping[dsl.Feature, parsmod.Feature],
    ) -> parsmod.Visitor:
        """Return the parser instance of this reader.

        Args:
            sources: Source mappings to be used by the parser.
            features: Feature mappings to be used by the parser.

        Returns:
            Parser instance.
        """

    @classmethod
    def format(
        cls, schema: dsl.Source.Schema, data: layout.Native  # pylint: disable=unused-argument
    ) -> layout.Tabular:
        """Format the input data into the required layout.Tabular format.

        Args:
            schema: Data schema.
            data: Input data.

        Returns:
            Data formatted into layout.Tabular format.
        """
        return layout.Dense.from_rows(data)

    @classmethod
    @abc.abstractmethod
    def read(cls, statement: parsmod.Source, **kwargs: typing.Any) -> layout.Native:
        """Perform the read operation with the given statement.

        Args:
            statement: Query statement in the reader's native syntax.
            kwargs: Optional reader keyword args.

        Returns:
            Data provided by the reader.
        """
