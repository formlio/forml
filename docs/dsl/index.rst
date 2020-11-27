 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
 ..   http://www.apache.org/licenses/LICENSE-2.0
 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Data Source DSL
===============

To allow projects :ref:`specifying <project-source>` their data requirements in a portable way, ForML comes with its
custom DSL (*domain-specific language*) that's at :doc:`runtime <../platform>` interpreted by the
:doc:`feeds subsystem <../feed>` to deliver the requested data.

The two main features of the DSL grammar is the `schema`_ declaration and the `query`_ syntax.

Schema
------

Schema is the abstract description of the particular datasource structure in terms of its column attributes (currently
a *name* and a *kind*). A schema is simply declared by extending the schema base class ``forml.io.dsl.struct.Schema``
and defining its fields as class attributes with values represented by ``forml.io.dsl.struct.Field``. For example::

    class Person(struct.Schema):
        """Base schema."""

        surname = struct.Field(kind.String())
        dob = struct.Field(kind.Date(), 'birthday')

    class Student(Person):
        """Extended schema."""

        level = struct.Field(kind.Integer())
        score = struct.Field(kind.Float())

Here we defined schemas of two potential datasources - a generic ``Person`` with a *string* field called ``surname`` and
a *date* field ``dob`` (aliased as ``birthday``) plus its extended version ``Student`` with two more fields -
*integer* ``level`` and *float* ``score``. The schema declaration API is based on the following rules:

* the default field name is the class attribute name unless explicitly defined as the ``Field`` parameter
* a field must be associated with one of the supported `kinds`_
* schemas can be extended
* extended fields can override same name fields from parents
* field ordering is based on the in-class definition order, fields from parent classes come before fields of child
classes, overriding a field doesn't change its position

Schemas are expected to be published in form of :ref:`catalogs <io-catalogized-schemas>` which can be imported by both
:doc:`projects <../project>` and :doc:`platforms <../platform>` making them the mapping intermediaries.

In :ref:`project sources <project-source>`, schemas can be used for specifying actual DSL *queries*. Any declared schema
is a fully *queryable* object so you can use all the `query`_ features as described below.

When referring to a schema field, one can use either the form of a attribute-getter like ``<Schema>.<field_name>`` or
alternatively (if for example the field name is not a valid python identifier) using the item-getter as
``<schema>['<field_name>']``.

Kinds
^^^^^

Following is the list of types (aka *kinds*) that can be used in schema field definitions:

.. autosummary::
   :toctree: _auto

   forml.io.dsl.struct.kind.Boolean
   forml.io.dsl.struct.kind.Integer
   forml.io.dsl.struct.kind.Float
   forml.io.dsl.struct.kind.Decimal
   forml.io.dsl.struct.kind.String
   forml.io.dsl.struct.kind.Date
   forml.io.dsl.struct.kind.Timestamp
   forml.io.dsl.struct.kind.Array
   forml.io.dsl.struct.kind.Map
   forml.io.dsl.struct.kind.Struct


Query
-----

The DSL allows to specify a rich ETL procedure of retrieving the data in any required shape or form. This can be
achieved through the *query* API that's available on top of any `schema`_ object. Important feature of the query syntax
is also the support for column `expressions`_.

Example query might look like::

    ETL = student.join(person, student.surname == person.surname)
            .join(school_ref, student.school == school_ref.sid)
            .select(student.surname.alias('student'), school_ref['name'], function.Cast(student.score, kind.String()))
            .where(student.score < 2)
            .orderby(student.level, student.score)
            .limit(10)


Following is the list of the query API methods:

.. autoattribute:: forml.io.dsl.struct.frame.Query.columns
.. automethod:: forml.io.dsl.struct.frame.Query.select
.. automethod:: forml.io.dsl.struct.frame.Query.join
.. automethod:: forml.io.dsl.struct.frame.Query.groupby
.. automethod:: forml.io.dsl.struct.frame.Query.having
.. automethod:: forml.io.dsl.struct.frame.Query.where
.. automethod:: forml.io.dsl.struct.frame.Query.limit
.. automethod:: forml.io.dsl.struct.frame.Query.orderby


Expressions
^^^^^^^^^^^

Any schema field representing a data column can be involved in a *column expression*. All the schema field objects
implement number native of operators, that can be used to directly form an expression. Furthermore, there are separate
function modules that can be imported to build more complex expressions.

The native operators available directly on the field instances are:

+--------------+-----------------------------------------------------------+
| Type         | Syntax                                                    |
+==============+===========================================================+
| Comparison   | ``==``, ``!=``, ``<``, ``<=``, ``>``, ``>=``              |
+--------------+-----------------------------------------------------------+
| Logical      | ``&``, ``|``, ``~``                                       |
+--------------+-----------------------------------------------------------+
| Arithmetical | ``+``, ``-``, ``*``, ``/``, ``%``                         |
+--------------+-----------------------------------------------------------+
| Alias        | .. automethod:: forml.io.dsl.struct.series.Operable.alias |
+--------------+-----------------------------------------------------------+


There is also a bunch of functions available to be used within the query expressions. They are grouped into the
following categories:

.. autosummary::
   :recursive:
   :toctree: _auto

   forml.io.dsl.function.aggregate
   forml.io.dsl.function.conversion
   forml.io.dsl.function.datetime
   forml.io.dsl.function.math
