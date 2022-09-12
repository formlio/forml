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
Payload utilities.
"""
import typing

from ._codec import Decoder, Encoder, Encoding, get_decoder, get_encoder
from ._external import Entry, Outcome, Request, Response
from ._internal import Dense, Tabular

#: Sequence of items (n-dimensional but only the top one needs to be accessible).
Array = typing.Sequence[typing.Any]
#: Sequence of columns of any type (columnar, column-wise semantic).
ColumnMajor = Array
#: Sequence of rows of any type (row-wise semantic).
RowMajor = Array
#: Generic type variable representing arbitrary native type.
Native = typing.TypeVar('Native')


__all__ = [
    'Array',
    'ColumnMajor',
    'Decoder',
    'Dense',
    'Encoder',
    'Encoding',
    'Entry',
    'get_encoder',
    'get_decoder',
    'Native',
    'Outcome',
    'Request',
    'Response',
    'RowMajor',
    'Tabular',
]
