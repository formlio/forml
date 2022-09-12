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
    """Schema field class.

    When defined as class attributes on a particular :class:`dsl.Schema <forml.io.dsl.Schema>`
    object, these instances represent the individual fields of the logical data source.

    Args:
        kind: Mandatory field data type.

              The value must be one of the :class:`dsl.Any <forml.io.dsl.Any>` data type instances.
        name: Explicit field name.
    """

    kind: 'dsl.Any'
    """Field data type."""
    name: typing.Optional[str] = None
    """Optional explicit field name.

    Implicitly defaults to the name of the schema class attribute holding this field.
    """

    def renamed(self, name: typing.Optional[str]) -> 'dsl.Field':
        """Return copy of the field with the new name.

        Args:
            name: New name to be used.

        Returns:
            New Field instance.
        """
        return self if name == self.name else Field(self.kind, name)


class Schema(metaclass=frame.Table):  # pylint: disable=invalid-metaclass
    """DSL frontend for table (schema) definitions.

    A Schema is a logical representation of a particular dataset. Together with the
    :class:`dsl.Field <forml.io.dsl.Field>`, this class provides the *schema definition* frontend
    API which can be used in two operational modes:

    Declarative Mode
        The primary approach for schema definition is based on the *class inheritance* syntax with
        individual class attributes declared as the :class:`dsl.Field <forml.io.dsl.Field>`
        instances representing the schema *fields*.

        This concept is based on the following rules:

        * the default *field name* is the class attribute name unless explicitly defined using the
          :attr:`dsl.Field.name <forml.io.dsl.Field.name>` parameter
        * schemas can be hierarchically extended further down
        * extended fields can override same-name fields from parents
        * field ordering is based on the in-class definition order, fields from parent classes
          come before fields of child classes; overriding a field does not change its position

        .. attention::
            To transparently provide the *query statement* interface on top of the defined schemas,
            the internal class handler magically turns all children inherited from ``dsl.Schema``
            to *instances* of :class:`dsl.Table <forml.io.dsl.Table>` (which itself has a
            :attr:`.schema <forml.io.dsl.Source.schema>` attribute derived from this class) instead
            of the intuitively expected *subclass* of the ``dsl.Schema`` parent.

    Functional Mode
        Additionally, schemas can be retrieved in a number of alternative ways implemented by the
        following factory methods:

        * :attr:`from_fields`
        * :attr:`from_record`
        * :attr:`from_path`

    Schema fields can either be referenced using the pythonic *attribute-getter* syntax like
    ``<Schema>.<field_name>`` or alternatively (e.g. if the field name is not a valid python
    identifier) using the *item-getter* syntax as ``<Schema>[<field_name>]``.

    Examples:
        Following is an example of the declarative syntax:

        .. code-block:: python

            class Person(dsl.Schema):
                '''Base schema.'''

                surname = dsl.Field(dsl.String())
                dob = dsl.Field(dsl.Date(), 'birthday')

            class Student(Person):
                '''Extended schema.'''

                level = dsl.Field(dsl.Integer())
                score = dsl.Field(dsl.Float())

        That's a declaration of two data sources - a generic ``Person`` with a *string* field called
        ``surname`` and a *date* field ``dob`` aliased as ``birthday`` plus its extended version
        ``Student`` with two more fields - *integer* ``level`` and *float* ``score``.

        This schema can be used to formulate a query statement as shown:

        >>> ETL = (
        ...     Student
        ...     .select(Student.surname.alias('name'), Student.dob)
        ...     .where(Student.score > 80)
        ... )
    """

    @staticmethod
    def from_fields(*fields: 'dsl.Field', title: typing.Optional[str] = None) -> 'dsl.Source.Schema':
        """Utility for functional schema assembly.

        Args:
            fields: Schema field list.
            title: Optional schema name.

        Returns:
            Assembled schema.

        Examples:
            >>> SCHEMA = dsl.Schema.from_fields(
            ...     dsl.Field(dsl.Integer(), name='A'),
            ...     dsl.Field(dsl.String(), name='B'),
            ... )
        """
        return frame.Source.Schema(title or 'Schema', tuple(), {f'_{i}': f for i, f in enumerate(fields)})

    @classmethod
    def from_record(
        cls, record: 'layout.Native', *names: str, title: typing.Optional[str] = None
    ) -> 'dsl.Source.Schema':
        """Utility for functional schema inference.

        Args:
            record: Scalar or vector representing a single record from which the schema should be
                    inferred.
            names: Optional field names.
            title: Optional schema name.

        Returns:
            Inferred schema.

        Examples:
            >>> SCHEMA = dsl.Schema.from_record(
            ...     ['foobar', 37], 'name', 'age', title='Person'
            ... )
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

        Returns:
            Imported schema table.

        Examples:
            >>> SCHEMA = dsl.Schema.from_path('foo.bar:Baz')
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
