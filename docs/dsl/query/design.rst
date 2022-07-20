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

.. _query-design:

Internal Design
===============

The DSL uses comprehensive class hierarchy to implement the desired API. Although the internal
design is irrelevant from the practical usability standpoint, it is essential for implementing
additional :ref:`parsers <query-parser>`.

.. _query-model:

Model
-----

The following class diagram outlines the API model:

.. md-mermaid::
    classDiagram
        Source <|-- Queryable
        Source <|-- Statement
        Statement <|-- Set
        Statement <|-- Query
        Queryable <|-- Query
        Queryable <|-- Origin
        Origin <|-- Join
        Origin <|-- Reference
        Origin <|-- Table

        class Source {
            <<abstract>>
            +Schema schema
            +list[Feature] features
            +reference() Reference
            +union() Set
            +intersection() Set
            +difference() Set
        }

        class Queryable {
            <<abstract>>
            +select() Query
            +where() Query
            +having() Query
            +groupby() Query
            +orderby() Query
            +limit() Query
        }

        class Statement {
            <<abstract>>
        }

        class Origin {
            <<abstract>>
            +join() Join
        }

        class Join {
            +Origin left
            +Origin right
            +Expression condition
            +Kind kind
        }

        class Set {
            +Statement left
            +Statement right
            +Kind kind
        }

        class Reference {
            +Source instance
            +str name
        }

        class Query {
            +Source source
            +list[Feature] selection
            +Expression prefilter
            +list[Operable] grouping
            +Expression postfilter
            +list[Operable] ordering
            +Rows rows
        }

        Feature <|-- Operable
        Feature <|-- Aliased
        Operable <|-- Literal
        Operable <|-- Element
        Element <|-- Column
        Operable <|-- Expression
        Expression <|-- Predicate

        class Feature {
            <<abstract>>
            +Any kind
            +alias() Aliased
        }

        class Operable {
            <<abstract>>
            +eq() Expression
            +ne() Expression
            +lt() Expression
            +le() Expression
            +gt() Expression
            +ge() Expression
            +and() Expression
            +or() Expression
            +not() Expression
            +add() Expression
            +sub() Expression
            +mul() Expression
            +div() Expression
            +mod() Expression
        }

        class Expression {
            <<abstract>>
        }

        class Predicate {
            <<abstract>>
        }

        class Aliased {
            +str name
            +Operable operable
        }

        class Element {
            +str name
            +Origin origin
        }

        class Column {
            +Table origin
        }

        class Literal {
            +Any value
        }


Notable Abstractions
^^^^^^^^^^^^^^^^^^^^

The hierarchy starts with the following two abstractions:

.. autoclass:: forml.io.dsl.Source
   :members: Schema, schema, features, reference, union, intersection, difference

.. autoclass:: forml.io.dsl.Queryable
   :members: select, where, having, groupby, orderby, limit

.. autoclass:: forml.io.dsl.Origin
   :members: join


.. autoclass:: forml.io.dsl.Feature
   :members: kind, alias

.. autoclass:: forml.io.dsl.Operable

.. autoclass:: forml.io.dsl.Predicate


Notable Final Types
^^^^^^^^^^^^^^^^^^^

.. autoclass:: forml.io.dsl.Set
.. autoclass:: forml.io.dsl.Query
.. autoclass:: forml.io.dsl.Join
   :members: Kind
.. autoclass:: forml.io.dsl.Reference
.. autoclass:: forml.io.dsl.Table
.. autoclass:: forml.io.dsl.Aliased



.. _query-parser:

Parser
------

.. autodata:: forml.io.dsl.parser.Source
.. autodata:: forml.io.dsl.parser.Feature
.. autoclass:: forml.io.dsl.parser.Visitor



For reference, several existing Parser implementations can be found under the
``forml.provider.feed.reader`` package:

.. autosummary::
   :nosignatures:

   forml.provider.feed.reader.sql.alchemy.Parser
   forml.provider.feed.reader.sql.dbapi.Parser


Exceptions
^^^^^^^^^^
