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

"""Hello World application descriptor."""
import json
import typing

import numpy

from forml import project as prj
from forml.io import dsl, layout
from forml.runtime import asset
from tests import helloworld


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
        project = registry.get(helloworld.PACKAGE.manifest.name)
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
