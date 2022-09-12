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
Codec tests.
"""
import pickle
import typing

import pytest

from forml.io import dsl, layout


class TestEncoding:
    """Encoding tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def encoding() -> layout.Encoding:
        """Encoding fixture."""
        return layout.Encoding('application/json', foo='1', bar='2')

    @pytest.mark.parametrize(
        'raw, kind, options',
        [
            ('application/json; charset=UTF-8', ['application/json'], [{'charset': 'UTF-8'}]),
            (
                'image/GIF; q=0.6; a=x, image/jpeg; q=0.6, text/html; q=1.0, text/*; q=0.8, image/*; q=0.5, */*; q=0.1',
                ['text/html', 'text/*', 'image/gif', 'image/jpeg', 'image/*', '*/*'],
                [{}, {}, {'a': 'x'}, {}, {}, {}],
            ),
        ],
    )
    def test_parse(self, raw: str, kind: typing.Sequence[str], options: typing.Sequence[typing.Mapping[str, str]]):
        """Encoding parsing test."""
        assert all(e == (k, o) for e, k, o in zip(layout.Encoding.parse(raw), kind, options))

    @pytest.mark.parametrize(
        'pattern, subject, matches',
        [
            ('application/json', 'application/json', True),
            ('application/json; foo=1; bar=2', 'application/json; bar=2; foo=1', True),
            ('application/json; foo=1; bar=2', 'application/json; bar=2', False),
            ('application/json', 'Application/JSON', True),
            ('application/json', 'foobar/json', False),
            ('application/*', 'application/json', True),
            ('*/*', 'application/json', True),
            ('*/*; foo=1', 'application/json', False),
            ('*/*', '*/json', False),
        ],
    )
    def test_match(self, pattern: str, subject: str, matches: bool):
        """Encoding matching test."""
        assert layout.Encoding.parse(pattern)[0].match(layout.Encoding.parse(subject)[0]) == matches

    def test_header(self, encoding: layout.Encoding):
        """Encoding header test."""
        assert layout.Encoding.parse(encoding.header)[0] == encoding

    def test_hashable(self, encoding: layout.Encoding):
        """Encoding hashability test."""
        assert hash(encoding)

    def test_serializable(self, encoding: layout.Encoding):
        """Encoding serializability test."""
        assert pickle.loads(pickle.dumps(encoding)) == encoding


SCHEMA = dsl.Schema.from_fields(dsl.Field(dsl.Integer(), name='A'), dsl.Field(dsl.String(), name='B'))
OUTCOME = layout.Outcome(SCHEMA, [[1, 'a'], [2, 'b'], [3, 'c']])


@pytest.mark.parametrize(
    'patterns, encoding, outcome, encoded, schema',
    [
        (
            'foo/bar, application/json; format=foobar, application/json; format=pandas-columns',
            layout.Encoding('application/json', format='pandas-columns'),
            OUTCOME,
            b'{"A":{"0":1,"1":2,"2":3},"B":{"0":"a","1":"b","2":"c"}}',
            None,
        ),
        (
            'application/json; format=foobar, */*',
            layout.Encoding('application/json', format='pandas-records'),
            OUTCOME,
            b'[{"A":1,"B":"a"},{"A":2,"B":"b"},{"A":3,"B":"c"}]',
            None,
        ),
        (
            'application/json; format=pandas-index',
            layout.Encoding('application/json', format='pandas-index'),
            OUTCOME,
            b'{"0":{"A":1,"B":"a"},"1":{"A":2,"B":"b"},"2":{"A":3,"B":"c"}}',
            None,
        ),
        (
            'application/json; format=pandas-split',
            layout.Encoding('application/json', format='pandas-split'),
            OUTCOME,
            b'{"columns":["A","B"],"data":[[1,"a"],[2,"b"],[3,"c"]]}',
            None,
        ),
        (
            '*/*; q=0.1, application/json; format=pandas-values',
            layout.Encoding('application/json', format='pandas-values'),
            OUTCOME,
            b'[[1,"a"],[2,"b"],[3,"c"]]',
            dsl.Schema.from_fields(dsl.Field(dsl.Integer(), name='0'), dsl.Field(dsl.String(), name='1')),
        ),
        (
            '*/*; q=0.1, text/csv',
            layout.Encoding('text/csv'),
            OUTCOME,
            b'A,B\n1,a\n2,b\n3,c\n',
            None,
        ),
        (  # Json.to_pandas list of row dicts
            None,
            layout.Encoding('application/json'),
            OUTCOME,
            b'[{"A":1,"B":"a"},{"A":2,"B":"b"},{"A":3,"B":"c"}]',
            None,
        ),
        (  # Json.to_pandas fallback to columns
            None,
            layout.Encoding('application/json'),
            OUTCOME,
            b'{"A":{"0":1,"1":2,"2":3},"B":{"0":"a","1":"b","2":"c"}}',
            None,
        ),
        (  # Json.to_pandas TF serving's "instances" format
            None,
            layout.Encoding('application/json'),
            OUTCOME,
            b'{"instances":[{"A":1,"B":"a"},{"A":2,"B":"b"},{"A":3,"B":"c"}]}',
            None,
        ),
        (  # Json.to_pandas TF serving's "inputs" format
            None,
            layout.Encoding('application/json'),
            OUTCOME,
            b'{"inputs":{"A":{"0":1,"1":2,"2":3},"B":{"0":"a","1":"b","2":"c"}}}',
            None,
        ),
    ],
)
def test_codec(
    patterns: typing.Optional[str],
    encoding: layout.Encoding,
    outcome: layout.Outcome,
    encoded: bytes,
    schema: typing.Optional[dsl.Source.Schema],
):
    """Encoder/Decoder getter test."""
    if patterns:
        encoder = layout.get_encoder(*layout.Encoding.parse(patterns))
        assert encoder.encoding == encoding
        assert encoder.dumps(outcome) == encoded
    decoder = layout.get_decoder(encoding)
    entry = decoder.loads(encoded)
    assert entry.schema == (schema or outcome.schema)
    assert entry.data.to_rows().tolist() == outcome.data
