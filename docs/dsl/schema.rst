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

.. _schema:

Schema Definition
=================

The schema definition API is the core part of the DSL. A schema is the virtual intermediary
allowing to decouple data solutions (ForML projects) from physical data instances, and :ref:`linking
each other <io-resolving>` directly only at runtime using selected :ref:`feed providers <feed>`.

To become available to both :ref:`projects <project>` and :ref:`platforms <platform>`, schemas
need to be published in form of :ref:`schema catalogs <io-catalog>`.
Once declared, schemas can be used to formulate complex :ref:`query statements <query>` -
most notably as formal descriptions of the :ref:`project data-source <project-source>`
requirements.

The schema definition API is based on the following structures:

.. autoclass:: forml.io.dsl.Schema
   :members: from_fields, from_record, from_path

.. autoclass:: forml.io.dsl.Field
   :members: kind, name


.. _dsl-kinds:

Type System
-----------

The DSL is using its own type system for its schema :attr:`Field <forml.io.dsl.Field.kind>`
definitions propagated into the query :attr:`Feature <forml.io.dsl.Feature.kind>` instances.

The type system is based on the following hierarchy:

.. md-mermaid::
    classDiagram
        Any <|-- Primitive
        Primitive <|-- Numeric
        Primitive <|-- Boolean
        Numeric <|-- Integer
        Numeric <|-- Float
        Numeric <|-- Decimal
        Primitive <|-- String
        Primitive <|-- Date
        Date <|-- Timestamp
        Any <|-- Compound
        Compound <|-- Array
        Compound <|-- Map
        Compound <|-- Struct

        <<abstract>> Any
        <<abstract>> Primitive
        <<abstract>> Numeric
        <<abstract>> Compound

        class Array {
            +Any element
        }

        class Map {
            +Any key
            +Any value
        }

        class Struct {
            +list[Element]
        }


Following is the description of the main types:

.. autoclass:: forml.io.dsl.Any
.. autoclass:: forml.io.dsl.Boolean
.. autoclass:: forml.io.dsl.Integer
.. autoclass:: forml.io.dsl.Float
.. autoclass:: forml.io.dsl.Decimal
.. autoclass:: forml.io.dsl.String
.. autoclass:: forml.io.dsl.Date
.. autoclass:: forml.io.dsl.Timestamp
.. autoclass:: forml.io.dsl.Array
.. autoclass:: forml.io.dsl.Map
.. autoclass:: forml.io.dsl.Struct
