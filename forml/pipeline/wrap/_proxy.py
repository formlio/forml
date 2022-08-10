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
Common wrapping functionality.
"""
import abc
import functools
import typing

Origin = typing.TypeVar('Origin')
Instance = typing.TypeVar('Instance')


class Type(typing.Generic[Origin, Instance], metaclass=abc.ABCMeta):
    """Abstract base class for wrapped instances acting as a *type-like constructor proxy* for the
    embedded entity.

    Instances take over the docstring and other identity attributes from the origin entity.

    Particular implementations are used internally by the :class:`wrap.Actor
    <forml.pipeline.wrap.Actor>` and the :class:`wrap.Operator <forml.pipeline.wrap.Operator>`
    facilities.

    Methods:
        __call__(*args, **kwargs):
            Constructor-like proxy method of the embedded origin entity.

            Args:
                args: Wrapped entity init positional arguments.
                kwargs: Wrapped entity init keyword arguments.

            Returns:
                Instance of the origin entity.
    """

    def __init__(self, origin: Origin):
        functools.update_wrapper(self, origin)
        if not callable(origin):
            delattr(self, '__wrapped__')  # otherwise fails on inspect.signature()

    @abc.abstractmethod
    def __call__(self, *args, **kwargs) -> Instance:
        """Actor constructor-like proxy."""
