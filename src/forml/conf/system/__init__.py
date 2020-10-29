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
System configuration.
"""

import contextlib
import pathlib
import sys
import typing


@contextlib.contextmanager
def path(*directories: typing.Union[str, pathlib.Path]) -> typing.Iterable[None]:
    """Context manager for putting given paths on python module search path but only for the duration of the context.

    Args:
        *directories: Paths to be inserted to sys.path when in the context.

    Returns: Context manager.
    """
    original = list(sys.path)
    for item in directories:
        sys.path.insert(0, str(pathlib.Path(item).resolve()))
    yield
    sys.path = original
