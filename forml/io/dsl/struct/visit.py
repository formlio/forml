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
Schema visitor APIs.
"""
import abc
import typing

from forml.io.dsl.struct import series

if typing.TYPE_CHECKING:
    from forml.io.dsl.struct import frame


class Columnar(metaclass=abc.ABCMeta):
    """Base class for both Frame and Series visitors."""

    @abc.abstractmethod
    def visit_table(self, origin: 'frame.Table') -> None:
        """Table hook.

        Args:
            origin: Source instance to be visited.
        """

    @abc.abstractmethod
    def visit_reference(self, origin: 'frame.Reference') -> None:
        """Reference hook.

        Args:
            origin: Instance to be visited.
        """


class Frame(Columnar):
    """Frame visitor."""

    def visit_source(self, source: 'frame.Source') -> None:  # pylint: disable=unused-argument, no-self-use
        """Generic source hook.

        Args:
            source: Source instance to be visited.
        """

    def visit_table(self, origin: 'frame.Table') -> None:
        """Table hook.

        Args:
            origin: Source instance to be visited.
        """
        self.visit_source(origin)

    def visit_reference(self, origin: 'frame.Reference') -> None:
        """Reference hook.

        Args:
            origin: Instance to be visited.
        """
        origin.instance.accept(self)
        self.visit_source(origin)

    def visit_join(self, source: 'frame.Join') -> None:
        """Join hook.

        Args:
            source: Instance to be visited.
        """
        source.left.accept(self)
        source.right.accept(self)
        self.visit_source(source)

    def visit_set(self, source: 'frame.Set') -> None:
        """Set hook.

        Args:
            source: Instance to be visited.
        """
        source.left.accept(self)
        source.right.accept(self)
        self.visit_source(source)

    def visit_query(self, source: 'frame.Query') -> None:
        """Query hook.

        Args:
            source: Instance to be visited.
        """
        source.source.accept(self)
        self.visit_source(source)


class Series(Columnar):
    """Series visitor."""

    def visit_origin(self, origin: 'frame.Origin') -> None:  # pylint: disable=unused-argument, no-self-use
        """Tangible source hook.

        Args:
            origin: Tangible source instance to be visited.
        """

    def visit_table(self, origin: 'frame.Table') -> None:
        """Table hook.

        Args:
            origin: Source instance to be visited.
        """
        self.visit_origin(origin)

    def visit_reference(self, origin: 'frame.Reference') -> None:
        """Reference hook.

        Args:
            origin: Instance to be visited.
        """
        self.visit_origin(origin)

    def visit_column(self, column: 'series.Column') -> None:  # pylint: disable=unused-argument, no-self-use
        """Generic column hook.

        Args:
            column: Column instance to be visited.
        """

    def visit_aliased(self, column: 'series.Aliased') -> None:
        """Generic expression hook.

        Args:
            column: Aliased column instance to be visited.
        """
        column.operable.accept(self)
        self.visit_column(column)

    def visit_element(self, column: 'series.Element') -> None:
        """Generic expression hook.

        Args:
            column: Element instance to be visited.
        """
        column.origin.accept(self)
        self.visit_column(column)

    def visit_literal(self, column: 'series.Literal') -> None:
        """Generic literal hook.

        Args:
            column: Literal instance to be visited.
        """
        self.visit_column(column)

    def visit_expression(self, column: 'series.Expression') -> None:
        """Generic expression hook.

        Args:
            column: Expression instance to be visited.
        """
        for term in column:
            if isinstance(term, series.Column):
                term.accept(self)
        self.visit_column(column)

    def visit_window(self, column: 'series.Window') -> None:
        """Generic window hook.

        Args:
            column: Window instance to be visited.
        """
        self.visit_column(column)
