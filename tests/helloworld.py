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

"""Hello World schemas."""
import json
import multiprocessing
import pathlib
import typing
import uuid

import numpy

from forml import io
from forml import project as prj
from forml.io import dsl, layout
from forml.io.dsl import parser as parsmod
from forml.runtime import asset


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


PACKAGE = prj.Package(pathlib.Path(__file__).parent / 'helloworld.4ml')
TRAINSET = (
    ('smith', 'oxford', 1, 1),
    ('black', 'cambridge', 2, 1),
    ('harris', 'stanford', 3, 3),
)
TRAINSET_FEATURES = layout.Dense.from_rows(TRAINSET).take_columns([0, 1, 2]).to_rows()
TRAINSET_LABELS = layout.Dense.from_rows(TRAINSET).to_columns()[-1]
TESTSET = TRAINSET_FEATURES
GENERATION_PREDICTION = 3, 6, 9


class Registry(asset.Registry):
    """Fixture registry implementation."""

    ContentT = dict[
        asset.Project.Key,
        dict[
            asset.Release.Key,
            tuple[prj.Package, dict[asset.Generation.Key, tuple[asset.Tag, tuple[bytes]]]],
        ],
    ]

    def __init__(self, content: ContentT, unbound: dict[uuid.UUID, bytes], lock: multiprocessing.Lock):
        super().__init__()
        self._lock: multiprocessing.Lock = lock
        self._content: Registry.ContentT = content
        self._unbound: dict[uuid.UUID, bytes] = unbound

    def projects(self) -> typing.Iterable[str]:
        with self._lock:
            return self._content.keys()

    def releases(self, project: asset.Project.Key) -> typing.Iterable[asset.Release.Key]:
        with self._lock:
            return self._content[project].keys()

    def generations(
        self, project: asset.Project.Key, release: asset.Release.Key
    ) -> typing.Iterable[asset.Generation.Key]:
        try:
            with self._lock:
                return self._content[project][release][1].keys()
        except KeyError as err:
            raise asset.Level.Invalid(f'Invalid release ({release})') from err

    def pull(self, project: asset.Project.Key, release: asset.Release.Key) -> prj.Package:
        with self._lock:
            return self._content[project][release][0]

    def push(self, package: prj.Package) -> None:
        raise NotImplementedError()

    def read(
        self,
        project: asset.Project.Key,
        release: asset.Release.Key,
        generation: asset.Generation.Key,
        sid: uuid.UUID,
    ) -> bytes:
        with self._lock:
            if sid not in self._content[project][release][1][generation][0].states:
                raise asset.Level.Invalid(f'Invalid state id ({sid})')
            idx = self._content[project][release][1][generation][0].states.index(sid)
            return self._content[project][release][1][generation][1][idx]

    def write(self, project: asset.Project.Key, release: asset.Release.Key, sid: uuid.UUID, state: bytes) -> None:
        with self._lock:
            self._unbound[sid] = state

    def open(
        self, project: asset.Project.Key, release: asset.Release.Key, generation: asset.Generation.Key
    ) -> asset.Tag:
        try:
            with self._lock:
                return self._content[project][release][1][generation][0]
        except KeyError as err:
            raise asset.Level.Invalid(f'Invalid generation ({release}.{generation})') from err

    def close(
        self,
        project: asset.Project.Key,
        release: asset.Release.Key,
        generation: asset.Generation.Key,
        tag: asset.Tag,
    ) -> None:
        with self._lock:
            assert set(tag.states).issubset(self._unbound.keys())
            assert (
                project in self._content
                and release in self._content[project]
                and generation not in self._content[project][release][1]
            )
            self._content[project][release][1][generation] = (tag, tuple(self._unbound[k] for k in tag.states))
            self._unbound.clear()


class Feed(io.Feed[str, str]):
    """Dummy feed for unit-testing purposes."""

    class Reader(io.Feed.Reader[str, str, layout.RowMajor]):
        """Dummy reader that returns either the trainset or testset fixtures."""

        class Parser(parsmod.Visitor[str, str]):
            """Dummy parser that returns string keyword of `trainset` or `testset` depending on the number
            of projected columns."""

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
            Student.join(Person, Student.surname == Person.surname).source: 'pupil',
            Person: 'person',
            Student: 'student',
            School: 'school',
        }


class HelloWorld(prj.Descriptor):
    """Helloworld application descriptor."""

    JSON = 'application/json'

    @classmethod
    def decode(cls, request: layout.Request) -> layout.Request.Decoded:
        assert request.encoding == cls.JSON
        data = json.loads(request.payload)
        first = tuple(data[0].items())
        values = [[r[k] for k, _ in first] for r in data]
        fields = (dsl.Field(dsl.reflect(v), name=k) for k, v in first)
        return layout.Request.Decoded(dsl.Schema.from_fields(*fields), layout.Dense.from_rows(values))

    @classmethod
    def encode(
        cls, outcome: layout.Outcome, encoding: typing.Sequence[layout.Encoding], scope: typing.Any
    ) -> layout.Response:
        assert {cls.JSON, '*/*'}.intersection(encoding)
        if isinstance(outcome.data[0], (typing.Sequence, numpy.ndarray)):  # 2D
            assert len(outcome.schema) == len(outcome.data[0])
            values = [{f.name: v for f, v in zip(outcome.schema, r)} for r in outcome.data]
        else:
            assert len(outcome.schema) == 1
            name = next(iter(outcome.schema)).name
            values = [{name: r} for r in outcome.data]
        return layout.Response(json.dumps(values).encode('utf-8'), cls.JSON)

    @classmethod
    def select(cls, registry: asset.Directory, scope: typing.Any, stats: layout.Stats) -> asset.Instance:
        project = registry.get(PACKAGE.manifest.name)
        for release in reversed(project.list()):
            try:
                generation = project.get(release).list().last
            except asset.Level.Listing.Empty:
                continue
            break
        else:
            raise asset.Level.Listing.Empty(f'No models available for {project.key}')
        return asset.Instance(
            registry=registry,
            project=project.key,
            release=release,  # pylint: disable=undefined-loop-variable
            generation=generation,
        )


prj.setup(HelloWorld)


class Inventory(asset.Inventory):
    """Fixture inventory implementation."""

    def __init__(self, descriptors: typing.Iterable[type[prj.Descriptor]]):
        self._content: dict[str, type[prj.Descriptor]] = {d.application: d for d in descriptors}

    def list(self) -> typing.Iterable[str]:
        return self._content.keys()

    def get(self, application: str) -> type[prj.Descriptor]:
        return self._content[application.lower()]

    def put(self, descriptor: prj.Descriptor.Handle) -> None:
        self._content[descriptor.descriptor.application] = descriptor.descriptor
