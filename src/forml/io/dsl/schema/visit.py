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

import typing

if typing.TYPE_CHECKING:
    from forml.io.dsl.schema import frame, series


class Source:
    """Frame visitor.
    """
    def visit_source(self, source: 'frame.Source') -> None:
        """Generic source hook.

        Args:
            source: Source instance to be visited.
        """
        raise NotImplementedError(f'Unexpected call to {self.__class__.__name__}.visit_source')

    def visit_table(self, source: 'frame.Table') -> None:
        """Table hook.

        Args:
            source: Source instance to be visited.
        """
        self.visit_source(source)

    def visit_join(self, source: 'frame.Join') -> None:
        """Join hook.

        Args:
            source: Instance to be visited.
        """
        self.visit_source(source)

    def visit_set(self, source: 'frame.Set') -> None:
        """Set hook.

        Args:
            source: Instance to be visited.
        """
        self.visit_source(source)

    def visit_query(self, source: 'frame.Query') -> None:
        """Query hook.

        Args:
            source: Instance to be visited.
        """
        self.visit_source(source)

    def visit_reference(self, source: 'frame.Reference') -> None:
        """Reference hook.

        Args:
            source: Instance to be visited.
        """
        self.visit_source(source)


class Column(Source):
    """Series visitor.
    """
    def visit_column(self, column: 'series.Column') -> None:
        """Generic column hook.

        Args:
            column: Column instance to be visited.
        """
        raise NotImplementedError(f'Unexpected call to {self.__class__.__name__}.visit_column')

    def visit_aliased(self, column: 'series.Aliased') -> None:
        """Generic expression hook.

        Args:
            column: Aliased column instance to be visited.
        """
        self.visit_column(column)

    def visit_field(self, column: 'series.Field') -> None:
        """Generic expression hook.

        Args:
            column: Field instance to be visited.
        """
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
        self.visit_column(column)
