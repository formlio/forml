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
Flow members represent partial pipeline blocks during pipeline assembly.
"""
import abc
import typing
import weakref

from .. import _exception
from . import assembly

if typing.TYPE_CHECKING:
    from forml import flow


class Composable(metaclass=abc.ABCMeta):
    """Common interface for operators and expressions."""

    def __rshift__(self, right: 'flow.Composable') -> 'Compound':
        """Semantical composition construct."""
        return Compound(right, self)

    def __repr__(self):
        return self.__class__.__name__

    @abc.abstractmethod
    def compose(self, scope: 'flow.Composable') -> 'flow.Trunk':
        """Implementation of the internal task graph and its composition with the preceding part of
        the expression.

        Args:
            scope: Preceding part of the expression that this operator is supposed to compose with.

        Returns:
            Trunk instance representing the composed task graph.
        """

    @abc.abstractmethod
    def expand(self) -> 'flow.Trunk':
        """Compose this instance and the entire preceding part of the expression and return the
        resulting trunk.

        This is typically called by a downstream operator (*right* side of the expression) within
        its :meth:`compose` method where this is passed as part of the *left* side of the
        expression.

        Returns:
            Trunk instance representing the composed task graph.
        """


class Origin(Composable):
    """Initial builder without a predecessor."""

    def compose(self, scope: 'flow.Composable') -> 'flow.Trunk':
        """Origin composition is just the left side trunk.

        Args:
            scope: Left side composable.

        Returns:
            Trunk.
        """
        return scope.expand()

    def expand(self) -> 'flow.Trunk':
        return assembly.Trunk()


class Operator(Composable, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for operator implementations."""

    def expand(self) -> 'flow.Trunk':
        return self.compose(Origin())


class Compound(Composable):
    """Operator chaining descriptor."""

    _TERMS = weakref.WeakValueDictionary()

    def __init__(self, right: 'flow.Composable', left: 'flow.Composable'):
        for term in (left, right):
            if not isinstance(term, Composable):
                raise ValueError(f'{type(term)} not composable')
            if term in self._TERMS:
                raise _exception.TopologyError(f'Non-linear {term} composition')
            self._TERMS[term] = self
        self._right: 'flow.Composable' = right
        self._left: 'flow.Composable' = left

    def __repr__(self):
        return f'{self._left} >> {self._right}'

    def compose(self, scope: 'flow.Composable') -> 'flow.Trunk':
        """Expression composition is just extension of its segments.

        Args:
            scope: Left side composable.

        Returns:
            Trunk.
        """
        return scope.expand().extend(*self.expand())

    def expand(self) -> 'flow.Trunk':
        return self._right.compose(self._left)
