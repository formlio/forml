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
Data casting operators.
"""
import typing

from pandas.core import generic as pdtype

from forml.lib.flow.actor import wrapped, ndframe as ndfmod
from forml.lib.flow.operator import generic


@generic.Adapter.apply
@generic.Adapter.train
@generic.Adapter.label
@wrapped.Function.actor
def ndframe(data: typing.Any, columns: typing.Optional[typing.Sequence[str]] = None) -> pdtype.NDFrame:
    """Simple 1:1 operator that attempts to convert the data on each of apply/train/label path to pandas
    dataframe/series.

    Args:
        data: Input data.
        columns: Optional column names.

    Returns:
        Pandas dataframe/series.
    """
    return ndfmod.cast(data, columns=columns)
