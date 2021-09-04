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
Common parser tests.
"""
# pylint: disable=no-self-use
import abc

import pytest

from forml.io.dsl import parser as parsmod, function
from forml.io.dsl.struct import frame as framod, kind as kindmod, series as sermod


class TupleParser(metaclass=abc.ABCMeta):
    """Base class for testing a special parser implementations producing a tuples of parsed symbols."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='function')
    def parser() -> parsmod.Frame:
        """Parser fixture."""

    def format(self, result: parsmod.Source) -> tuple:
        """Post-format the parser output."""
        return result

    def test_parsing(
        self,
        query: framod.Query,
        student: framod.Table,
        school: framod.Table,
        school_ref: framod.Reference,
        parser: parsmod.Frame,
    ):
        """Parsing test."""
        with parser.switch():
            query.accept(parser)
            result = self.format(parser.fetch())
        assert result[0][0] == ('foo',)
        assert result[1] == (
            (((student,), (student.surname,)), 'student'),
            (('bar',), (school_ref['name'],)),
            (function.Cast, ((student,), (student.score,)), kindmod.String()),
        )
        assert result[5] == (
            (((student,), ('baz',)), sermod.Ordering.Direction.ASCENDING),
            (((student,), (student.score,)), sermod.Ordering.Direction.ASCENDING),
        )
