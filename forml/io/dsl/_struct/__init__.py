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
DSL structures.
"""
import itertools
import typing

import forml

from . import frame
from . import kind as kindmod

if typing.TYPE_CHECKING:
    from forml.io import dsl, layout


class Field(typing.NamedTuple):
    """Schema field class."""

    kind: 'dsl.Any'
    name: typing.Optional[str] = None

    def renamed(self, name: typing.Optional[str]) -> 'dsl.Field':
        """Return copy of the field with the new name.

        Args:
            name: New name to be used.

        Returns:
            New Field instance.
        """
        return self if name == self.name else Field(self.kind, name)


class Schema(metaclass=frame.Table):  # pylint: disable=invalid-metaclass
    """Base class for table (schema) definitions.

    Note the meta class is actually going to turn it into an instance of frame.Table which itself has a ``.schema``
    attribute derived from this class and represented using ``dsl.Source.Schema``.
    """

    @staticmethod
    def from_fields(*fields: 'dsl.Field', title: typing.Optional[str] = None) -> 'dsl.Source.Schema':
        """Utility for programmatic schema assembly.

        Args:
            *fields: Schema field list.
            title: Optional schema name.

        Returns:
            Assembled schema.
        """
        return frame.Source.Schema(title or 'Schema', tuple(), {f'_{i}': f for i, f in enumerate(fields)})

    @classmethod
    def from_record(
        cls, record: 'layout.Native', *names: str, title: typing.Optional[str] = None
    ) -> 'dsl.Source.Schema':
        """Utility for programmatic schema inference.

        Args:
            record: Scalar or vector representing single record for which the schema should be inferred.
            names: Optional field names.
            title: Optional schema name.

        Returns:
            Inferred schema.
        """
        if not hasattr(record, '__len__') or isinstance(record, (str, bytes)):  # wrap if scalar
            record = [record]
        fields = (
            Field(kindmod.reflect(v), name=(str(n) if n is not None else f'c{i}'))
            for i, (v, n) in enumerate(itertools.zip_longest(record, names))
        )
        return cls.from_fields(*fields, title=title)

    @classmethod
    def from_path(cls, path: str) -> 'dsl.Table':
        """Utility for importing a schema table from the given path.

        Args:
            path: Schema path in form of ``full.module.path:schema.qualified.ClassName``.

        Returns: Imported schema table.
        """
        try:
            module, schema = path.split(':', 1)
        except ValueError as err:
            raise forml.InvalidError(f'Not a schema path: {path}') from err
        try:
            result = __import__(module, fromlist=[schema])
        except ModuleNotFoundError as err:
            raise forml.MissingError(f'No such module: {module}') from err
        for qualifier in schema.split('.'):
            try:
                result = getattr(result, qualifier)
            except AttributeError as err:
                raise forml.MissingError(f'No such schema: {schema}') from err
        if not isinstance(result, frame.Table):
            raise forml.InvalidError(f'Not a schema: {result}')
        return result
