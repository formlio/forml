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
ForML application encoding utils.
"""
import abc
import collections
import functools
import io
import json
import logging
import typing

import pandas

import forml
from forml.io import dsl, layout

LOGGER = logging.getLogger(__name__)


class Decoder(abc.ABC):
    """Decoder base class."""

    @abc.abstractmethod
    def loads(self, data: bytes) -> layout.Entry:
        """Decoder logic.

        Args:
            data: Bytes to decode.

        Returns: Decoded entry.
        """


class Encoder(abc.ABC):
    """Encoder base class."""

    @property
    @abc.abstractmethod
    def encoding(self) -> layout.Encoding:
        """Get the encoding produced by this encoder.

        Returns:
            Encoding instance.
        """

    @abc.abstractmethod
    def dumps(self, outcome: layout.Outcome) -> bytes:
        """Encoder logic.

        Args:
            outcome: Outcome to encode.

        Returns: Encoded entry.
        """


class Pandas:
    """Combo of Pandas based decoder/encoder wrappers."""

    class Schema:
        """Schema util."""

        _CACHE: dict[int, dsl.Source.Schema] = {}
        MAX_SAMPLE = 10

        @classmethod
        def from_frame(cls, frame: pandas.DataFrame) -> dsl.Source.Schema:
            """Infer the DSL schema from the given Pandas DataFrame.

            Args:
                frame: Non-empty pandas dataframe to infer the schema from.

            Returns:
                Inferred schema.
            """
            key = hash(tuple(frame.dtypes.items()))
            if key not in cls._CACHE:
                assert not frame.empty, 'Empty frame'
                # infer schema from a number of rows (MAX_SAMPLE) and take the most frequently occurring
                cls._CACHE[key] = collections.Counter(
                    dsl.Schema.from_record(r, *frame.columns)
                    for r in frame.sample(min(len(frame), cls.MAX_SAMPLE)).itertuples(index=False)
                ).most_common(1)[0][0]
            return cls._CACHE[key]

    class Decoder(Decoder):
        """Pandas based decoder."""

        def __init__(self, converter: typing.Callable[[str], pandas.DataFrame]):
            self._converter: typing.Callable[[str], pandas.DataFrame] = converter

        def loads(self, data: bytes) -> layout.Entry:
            frame = self._converter(data.decode())
            schema = Pandas.Schema.from_frame(frame)
            return layout.Entry(schema, layout.Dense.from_rows(frame.values))

    class Encoder(Encoder):
        """Pandas based encoder."""

        def __init__(self, converter: typing.Callable[[pandas.DataFrame], str], encoding: layout.Encoding):
            self._converter: typing.Callable[[pandas.DataFrame], str] = converter
            self._encoding: layout.Encoding = encoding

        @property
        def encoding(self) -> layout.Encoding:
            return self._encoding

        @classmethod
        @functools.lru_cache(256)
        def _columns(cls, schema: dsl.Source.Schema) -> typing.Sequence[str]:
            """Get the column list for the given schema.

            Args:
                schema: Descriptor to extract the columns from.

            Returns:
                Columns from schema.
            """
            return tuple(f.name for f in schema)

        def dumps(self, outcome: layout.Outcome) -> bytes:
            return self._converter(pandas.DataFrame(outcome.data, columns=self._columns(outcome.schema))).encode()


class Json:
    """Json encoding utils."""

    @staticmethod
    def to_pandas(data: str) -> pandas.DataFrame:
        """Try decoding data as JSON returning it as pandas DataFrame.

        Args:
            data: Encoded json data.

        Returns:
            Pandas DataFrame of the decoded data.
        """
        src = json.loads(data)
        if isinstance(src, list):  # list of row dicts
            return pandas.DataFrame.from_records(src)
        if 'instances' in src:  # TF serving's "instances" format
            return pandas.DataFrame.from_records(src['instances'])
        if 'inputs' in src:  # TF serving's "inputs" format
            return pandas.DataFrame.from_dict(src['inputs'], orient='columns')
        #  fallback to columns
        return pandas.DataFrame.from_dict(src, orient='columns')


_JSON = 'application/json'
ENCODING_JSON_PANDAS_COLUMNS = layout.Encoding(_JSON, format='pandas-columns')
ENCODING_JSON_PANDAS_INDEX = layout.Encoding(_JSON, format='pandas-index')
ENCODING_JSON_PANDAS_RECORDS = layout.Encoding(_JSON, format='pandas-records')
ENCODING_JSON_PANDAS_SPLIT = layout.Encoding(_JSON, format='pandas-split')
ENCODING_JSON_PANDAS_TABLE = layout.Encoding(_JSON, format='pandas-table')
ENCODING_JSON_PANDAS_VALUES = layout.Encoding(_JSON, format='pandas-values')
ENCODING_JSON = layout.Encoding(_JSON)
ENCODING_CSV = layout.Encoding('text/csv')


ENCODERS: typing.Sequence[Encoder] = (
    Pandas.Encoder(functools.partial(pandas.DataFrame.to_json, orient='records'), ENCODING_JSON_PANDAS_RECORDS),
    Pandas.Encoder(functools.partial(pandas.DataFrame.to_json, orient='columns'), ENCODING_JSON_PANDAS_COLUMNS),
    Pandas.Encoder(functools.partial(pandas.DataFrame.to_json, orient='index'), ENCODING_JSON_PANDAS_INDEX),
    Pandas.Encoder(
        functools.partial(pandas.DataFrame.to_json, orient='split', index=False), ENCODING_JSON_PANDAS_SPLIT
    ),
    Pandas.Encoder(
        functools.partial(pandas.DataFrame.to_json, orient='table', index=False), ENCODING_JSON_PANDAS_TABLE
    ),
    Pandas.Encoder(functools.partial(pandas.DataFrame.to_json, orient='values'), ENCODING_JSON_PANDAS_VALUES),
    Pandas.Encoder(functools.partial(pandas.DataFrame.to_csv, index=False), ENCODING_CSV),
)


DECODERS: typing.Sequence[tuple[Decoder, layout.Encoding]] = (
    (Pandas.Decoder(functools.partial(pandas.read_json, orient='columns')), ENCODING_JSON_PANDAS_COLUMNS),
    (Pandas.Decoder(functools.partial(pandas.read_json, orient='index')), ENCODING_JSON_PANDAS_INDEX),
    (Pandas.Decoder(functools.partial(pandas.read_json, orient='records')), ENCODING_JSON_PANDAS_RECORDS),
    (Pandas.Decoder(functools.partial(pandas.read_json, orient='split')), ENCODING_JSON_PANDAS_SPLIT),
    (Pandas.Decoder(functools.partial(pandas.read_json, orient='table')), ENCODING_JSON_PANDAS_TABLE),
    (Pandas.Decoder(functools.partial(pandas.read_json, orient='values')), ENCODING_JSON_PANDAS_VALUES),
    (Pandas.Decoder(Json.to_pandas), ENCODING_JSON),
    (Pandas.Decoder(lambda v: pandas.read_csv(io.StringIO(v))), ENCODING_CSV),
)


@functools.lru_cache(256)
def get_decoder(source: layout.Encoding) -> Decoder:
    """Get a decoder suitable for the given source encoding.

    Args:
        source: Explicit encoding (no wildcards expected) to find a decoder for.

    Returns:
        Decoder for the given source encoding.
    """
    for codec, encoding in DECODERS:
        if encoding.match(source):
            return codec
    raise forml.MissingError(f'No decoder for {source}')


@functools.lru_cache(256)
def get_encoder(*targets: layout.Encoding) -> Encoder:
    """Get an encoder capable of producing one of the given target encodings.

    Args:
        targets: Encoding patterns (wildcards possible) to be produced by the matched encoder.

    Returns:
        Encoder for one of the given target encoding.
    """
    for pattern in targets:
        for codec in ENCODERS:
            if pattern.match(codec.encoding):
                return codec
    raise forml.MissingError(f'No encoder for any of {targets}')
