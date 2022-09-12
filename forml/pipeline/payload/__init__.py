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
General payload manipulation utilities.

ForML is by design fairly :ref:`payload-format agnostic <io-payload>` leaving the choice of
compatible operators/actors to the implementer.

This module provides a number of generic payload-related operators to be parameterized with
particular actor implementations targeting different payload formats.

Note:
    For convenience, there is also a couple of payload-specific actors designed to be engaged
    only with that particular payload format (typically :class:`pandas:pandas.DataFrame`). This
    does not make that format any more preferable from the general ForML perspective as it still
    maintains its payload format neutrality.
"""

from ._convert import ToPandas, pandas_params
from ._debug import Dump, Dumpable, PandasCSVDumper, Sniff
from ._generic import Apply, MapReduce, PandasConcat, PandasDrop, PandasSelect
from ._split import CrossValidable, CVFoldable, PandasCVFolds

__all__ = [
    'Apply',
    'CrossValidable',
    'CVFoldable',
    'Dump',
    'Dumpable',
    'MapReduce',
    'PandasConcat',
    'PandasCSVDumper',
    'PandasCVFolds',
    'PandasDrop',
    'pandas_params',
    'PandasSelect',
    'Sniff',
    'ToPandas',
]
