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
Consumer implementation.
"""

import abc
import logging
import typing

from forml import flow
from forml.io import dsl, layout

LOGGER = logging.getLogger(__name__)


class Writer(typing.Generic[layout.Native], metaclass=abc.ABCMeta):
    """Base class for writer implementation."""

    def __init__(self, schema: typing.Optional[dsl.Source.Schema], **kwargs: typing.Any):
        self._schema: typing.Optional[dsl.Source.Schema] = schema
        self._kwargs: typing.Mapping[str, typing.Any] = kwargs

    def __repr__(self):
        return flow.name(self.__class__, **self._kwargs)

    def __call__(self, data: layout.RowMajor) -> layout.Outcome:
        if not self._schema:
            LOGGER.warning('Inferring unknown output schema')
            self._schema = dsl.Schema.from_record(data[0])
        LOGGER.debug('Starting to publish')
        self.write(self.format(self._schema, data), **self._kwargs)
        return layout.Outcome(self._schema, data)

    @classmethod
    def format(
        cls, schema: dsl.Source.Schema, data: layout.RowMajor  # pylint: disable=unused-argument
    ) -> layout.Native:
        """Format the output data into the required payload.Native format.

        Args:
            schema: Product schema.
            data: Output data.

        Returns:
            Data formatted into layout.Native format.
        """
        return data

    @classmethod
    @abc.abstractmethod
    def write(cls, data: layout.Native, **kwargs: typing.Any) -> None:
        """Perform the write operation with the given data.

        Args:
            data: Output data in the writer's native format.
            kwargs: Optional writer keyword args.
        """
