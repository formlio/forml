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

To allow :ref:`projects to declare <project-source>` their data requirements in a portable way
independently of any particular storage technology and data formats, ForML comes with its custom
*Data Source DSL* (domain-specific language) which gets interpreted at :ref:`runtime
<platform-execution>` by the :doc:`feeds subsystem <feed>` performing the :ref:`content resolution
<io-resolving>` routine to deliver the requested datasets.

Conceptually, it is an *internal* DSL (i.e. within Python grammar) based on *declarative* style of
specifying the data profiles using the following two main constructs:

* :ref:`schema definition <dsl-schema>` syntax for logical representation of individual datasets
* :ref:`query statement <dsl-query>` notation for declaring the project data requirements

.. important::
    Do not confuse the DSL with an ORM framework. The DSL entities are not used to manage any data
    directly. Its sole purpose is to describe the data sources independently of data access
    mechanism in the same way the :ref:`workflow expression <workflow-expression>` describe the
    processing logic independently of the execution mechanism. In both cases, the abstract
    descriptions get *transcoded* to runtime-specific instructions when launched.

.. _dsl-schema:

Schema Definition
-----------------

The schema definition API is the core part of the DSL. A schema is the virtual intermediary
allowing to decouple data solutions (ForML projects) from physical data instances, and :ref:`linking
each other <io-resolving>` directly only at runtime using selected :doc:`feed providers <feed>`.

To become available to both :doc:`projects <project>` and :doc:`platforms <platform>`, schemas
need to be published in form of :ref:`schema catalogs <io-catalog>`.
Once declared, schemas can be used to formulate complex :ref:`query statements <dsl-query>` -
most notably as formal descriptions of the :ref:`project data-source <project-source>`
requirements.

The schema definition API is based on the following structures:

.. autoclass:: forml.io.dsl.Schema
   :members: from_fields, from_record, from_path

.. autoclass:: forml.io.dsl.Field
   :members: kind, name


.. _dsl-kinds:

Type System - DSL Kinds
^^^^^^^^^^^^^^^^^^^^^^^

Following is the list of types (aka *kinds*) that can be used in schema field definitions:

.. autosummary::
   :nosignatures:

   forml.io.dsl.Boolean
   forml.io.dsl.Integer
   forml.io.dsl.Float
   forml.io.dsl.Decimal
   forml.io.dsl.String
   forml.io.dsl.Date
   forml.io.dsl.Timestamp
   forml.io.dsl.Array
   forml.io.dsl.Map
   forml.io.dsl.Struct


.. _dsl-query:

Query Statement
---------------

TODO: explain SQL resemblance

Base Primitives
^^^^^^^^^^^^^^^

.. autoclass:: forml.io.dsl.Source
.. autoclass:: forml.io.dsl.Feature

Parser
^^^^^^

.. autoclass:: forml.io.dsl.parser.Visitor


Syntax
^^^^^^

The DSL allows to specify a rich ETL procedure of retrieving the data in any required shape or form. This can be
achieved through the *query* API that's available on top of any `schema`_ object. Important feature of the query syntax
is also the support for column `expressions`_.

Following is the list of the query API methods:

.. autoclass:: forml.io.dsl.Query
   :members: features, schema, select, join, groupby, having, where, limit, orderby

Example query might look like::

    ETL = student.join(person, student.surname == person.surname)
            .join(school_ref, student.school == school_ref.sid)
            .select(student.surname.alias('student'), school_ref['name'], function.Cast(student.score, kind.String()))
            .where(student.score < 2)
            .orderby(student.level, student.score)
            .limit(10)

Expressions
"""""""""""

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
| Alias        | .. automethod:: forml.io.dsl.Operable.alias               |
+--------------+-----------------------------------------------------------+


Functions
"""""""""

There is also a bunch of functions available to be used within the query expressions:

xx.. automodule:: forml.io.dsl.function
xx   :imported-members:
xx   :members:
