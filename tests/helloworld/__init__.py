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

"""Hello World test project helpers."""
import pathlib
import typing

from forml import application as appmod
from forml import io
from forml import project as prjmod
from forml.io import asset, dsl, layout
from forml.io.dsl import parser as parsmod


class Person(dsl.Schema):
    """Base table."""

    surname = dsl.Field(dsl.String())
    dob = dsl.Field(dsl.Date(), 'birthday')


class Student(Person):
    """Extended table."""

    level = dsl.Field(dsl.Integer())
    score = dsl.Field(dsl.Float())
    school = dsl.Field(dsl.Integer())
    updated = dsl.Field(dsl.Timestamp())


class School(dsl.Schema):
    """School table."""

    sid = dsl.Field(dsl.Integer(), 'id')
    name = dsl.Field(dsl.String())


PACKAGE = prjmod.Package(pathlib.Path(__file__).parent / 'package.4ml')
TRAINSET = (
    ('smith', 'oxford', 1, 1),
    ('black', 'cambridge', 2, 1),
    ('harris', 'stanford', 3, 3),
)
TRAINSET_FEATURES = layout.Dense.from_rows(TRAINSET).take_columns([0, 1, 2]).to_rows()
TRAINSET_LABELS = layout.Dense.from_rows(TRAINSET).to_columns()[-1]
TESTSET = TRAINSET_FEATURES
GENERATION_PREDICTION = 3, 6, 9


class Feed(io.Feed[str, str]):
    """Dummy feed for unit-testing purposes."""

    class Reader(io.Feed.Reader[str, str, layout.RowMajor]):
        """Dummy reader that returns either the trainset or testset fixtures."""

        class Parser(parsmod.Visitor[str, str]):
            """Dummy parser that returns string keyword of `trainset` or `testset` depending on the number
            of projected columns."""

            # pylint: disable=unnecessary-lambda-assignment
            resolve_feature = (
                generate_alias
            ) = generate_expression = generate_join = generate_literal = generate_set = lambda *_: ''
            generate_reference = lambda *_: ('', '')

            def generate_element(self, origin: str, element: str) -> str:
                return f'{origin}-{element}'

            def generate_query(
                self,
                source: str,
                features: typing.Sequence[str],
                where: typing.Optional[str],
                groupby: typing.Sequence[str],
                having: typing.Optional[str],
                orderby: typing.Sequence[tuple[str, dsl.Ordering.Direction]],
                rows: typing.Optional[dsl.Rows],
            ) -> str:
                return 'testset' if len(features) == len(TESTSET[0]) else 'trainset'

        @classmethod
        def parser(
            cls,
            sources: typing.Mapping[dsl.Source, parsmod.Source],
            features: typing.Mapping[dsl.Feature, parsmod.Feature],
        ) -> parsmod.Visitor:
            return cls.Parser(sources, features)  # pylint: disable=abstract-class-instantiated

        @classmethod
        def read(cls, statement: str, **kwargs: typing.Any) -> layout.RowMajor:
            return TESTSET if statement == 'testset' else TRAINSET

    def __init__(self, identity: str, **readerkw):
        super().__init__(**readerkw)
        self.identity: str = identity

    @property
    def sources(self) -> typing.Mapping[dsl.Source, parsmod.Source]:
        """Abstract method implementation."""
        return {
            Student.inner_join(Person, Student.surname == Person.surname): 'pupil',
            Person: 'person',
            Student: 'student',
            School: 'school',
        }


class Inventory(asset.Inventory):
    """Fixture inventory implementation."""

    def __init__(self, descriptors: typing.Iterable[appmod.Descriptor]):
        self._content: dict[str, appmod.Descriptor] = {d.name: d for d in descriptors}

    def list(self) -> typing.Iterable[str]:
        return self._content.keys()

    def get(self, application: str) -> appmod.Descriptor:
        return self._content[application.lower()]

    def put(self, descriptor: appmod.Descriptor.Handle) -> None:
        self._content[descriptor.descriptor.name] = descriptor.descriptor
