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
ForML application encoding tests.
"""
import typing

import pytest

from forml.io import dsl, layout
from forml.lib import application

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
        encoder = application.get_encoder(*layout.Encoding.parse(patterns))
        assert encoder.encoding == encoding
        assert encoder.dumps(outcome) == encoded
    decoder = application.get_decoder(encoding)
    entry = decoder.loads(encoded)
    assert entry.schema == (schema or outcome.schema)
    assert entry.data.to_rows().tolist() == outcome.data
