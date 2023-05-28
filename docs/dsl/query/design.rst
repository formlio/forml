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

The DSL uses a comprehensive class hierarchy to implement the desired API. Although the internal
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
            +inner_join() Join
            +left_join() Join
            +right_join() Join
            +full_join() Join
            +cross_join() Join
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
        Expression <|-- Window

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

        class Window {
            +Operable partition
        }

Base Abstractions
^^^^^^^^^^^^^^^^^

The hierarchy starts with the following two abstractions:

.. autoclass:: forml.io.dsl.Source
   :members: Schema, schema, features, reference, union, intersection, difference

.. autoclass:: forml.io.dsl.Feature
   :members: kind, alias


Notable Interfaces
^^^^^^^^^^^^^^^^^^

.. autoclass:: forml.io.dsl.Queryable
   :members: select, where, having, groupby, orderby, limit
   :show-inheritance:

.. autoclass:: forml.io.dsl.Origin
   :members: inner_join, left_join, right_join, full_join, cross_join
   :show-inheritance:

.. autoclass:: forml.io.dsl.Statement
   :show-inheritance:

.. autoclass:: forml.io.dsl.Operable
   :show-inheritance:

.. autoclass:: forml.io.dsl.Element
   :show-inheritance:

.. autoclass:: forml.io.dsl.Predicate


Notable Final Types
^^^^^^^^^^^^^^^^^^^

.. autoclass:: forml.io.dsl.Table
   :show-inheritance:

.. autoclass:: forml.io.dsl.Query
   :show-inheritance:

.. autoclass:: forml.io.dsl.Set
   :members: Kind
   :show-inheritance:

.. autoclass:: forml.io.dsl.Join
   :members: Kind
   :show-inheritance:

.. autoclass:: forml.io.dsl.Rows

.. autoclass:: forml.io.dsl.Reference
   :show-inheritance:

.. autoclass:: forml.io.dsl.Column
   :show-inheritance:

.. autoclass:: forml.io.dsl.Aliased
   :show-inheritance:

.. autoclass:: forml.io.dsl.Ordering
   :members: Term, Direction, make

.. autoclass:: forml.io.dsl.Window
   :members: Function

Exceptions
^^^^^^^^^^

.. autoexception:: forml.io.dsl.GrammarError
   :show-inheritance:


.. _query-parser:

Parser
------

Since the constructed :ref:`DSL query statement <query>` is a generic descriptor with no means
of direct execution, the ETL process depends on a particular :class:`io.Feed.Reader
<forml.io.Feed.Reader>` implementation to *parse* that query into a set of instructions
corresponding to the selected :ref:`feed <feed>` and its target storage layer.


Generic Interface
^^^^^^^^^^^^^^^^^

.. autodata:: forml.io.dsl.parser.Source
.. autodata:: forml.io.dsl.parser.Feature
.. autoclass:: forml.io.dsl.parser.Visitor


Exceptions
^^^^^^^^^^

.. autoexception:: forml.io.dsl.UnprovisionedError
   :show-inheritance:
.. autoexception:: forml.io.dsl.UnsupportedError
   :show-inheritance:


References
^^^^^^^^^^

For reference, several existing ``Parser`` implementations can be found under the
``forml.provider.feed.reader`` package:

.. autosummary::
   :nosignatures:

   forml.provider.feed.reader.alchemy.Parser
