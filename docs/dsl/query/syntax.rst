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

.. _query-syntax:

Syntax Reference
================

Being implemented as a *python-internal* DSL, its high-level syntax is consequently a subset of the
native Python syntax embedding the actual :ref:`DSL API <query-design>`.

Even though it is not coupled via any inherent dependency, the DSL is designed to loosely resemble
the SQL ``SELECT`` statement syntax for its proven modeling effectiveness and its universal
recognition as the :spelling:word:`de facto` ETL standard. The abstract descriptive role of the
DSL with the separate runtime-specific :ref:`parsing stage <query-parser>` responsible for
converting the generic query into arbitrary native representation still allows to integrate any
:ref:`data access mechanisms <feed>` including non-sql based sources.


Grammar Notation
----------------

Bellow is the DSL syntax described using the BNF notation. For readability, it is not strictly
formal - leaving some of the terminal symbols out with just their conceptual descriptions (e.g.
``<count>``, ``<identifier>``, ``<literal>``, etc.) or references to their :ref:`API class
<query-design>` representations (e.g. ``<table>``, ``function.*``).

Substantial part of the DSL is the syntax for *expression* notation which is based on an extensive
collection of supported :ref:`functions and operators <query-functions>`.

The central component of any query is an existing  :class:`dsl.Table <forml.io.dsl.Table>`
instance defined using the :ref:`schema API <schema>` within its :ref:`schema catalog
<io-catalog>`.

The full DSL syntax is:

.. code-block:: BNF

    <source> ::= <origin> | <set> | <query>

    <origin> ::= <table> | <reference> | <join>

    <set> ::=
        <source>.union(<source>)
        | <source>.intersection(<source>)
        | <source>.difference(<source>)

    <query> ::=
        <queryable>.select(<feature_list>)
        | <queryable>.where(<predicate>)
        | <queryable>.having(<predicate>)
        | <queryable>.groupby(<operable_list>)
        | <queryable>.orderby(<ordering_list>)
        | <queryable>.limit(<count> [, <count>])

    <table> ::= schema instances defined using dsl.Schema

    <reference> ::= <source>.reference([<identifier>])

    <join> ::=
        <origin>.inner_join(<origin>, <predicate>)
        | <origin>.left_join(<origin>, <predicate>)
        | <origin>.right_join(<origin>, <predicate>)
        | <origin>.full_join(<origin>, <predicate>)
        | <origin>.cross_join(<origin>)

    <queryable> ::= <query> | <origin>

    <feature_list> ::= <feature> [, <feature_list> ]

    <predicate> ::= <comparison> | <logical>

    <operable_list> ::= <operable> [, <operable_list> ]

    <ordering_list> ::= <ordering> [, <ordering_list> ]

    <count> ::= natural number

    <identifier> ::= string of letters, digits and underscores starting with a letter

    <feature> ::= <operable> | <aliased>

    <operable> ::= <element> | <literal> | <expression>

    <ordering> ::= <operable> [, <direction>]

    <aliased> ::= <feature>.alias(<identifier>)

    <element> ::= <origin>.<identifier>

    <literal> ::= any Python literal value

    <expression> ::=
        <aggregate>
        | <comparison>
        | <conversion>
        | <datetime>
        | <logical>
        | <math>
        | <window_spec>

    <direction> ::= "asc" | "ascending" | "desc" | "descending"

    <comparison> ::=
        <operable> == <operable>
        | <operable> != <operable>
        | <operable> < <operable>
        | <operable> <= <operable>
        | <operable> > <operable>
        | <operable> >= <operable>

    <logical> ::=
        <operable> & <operable>
        | <operable> | <operable>
        | ~ <operable>

    <conversion> ::= function.Cast | ...

    <datetime> ::= function.Year | ...

    <math> :: =
        <arithmetic>
        | function.Abs
        | function.Ceil
        | function.Floor
        | ...

    <arithmetic> ::=
        <operable> + <operable>
        | <operable> - <operable>
        | <operable> * <operable>
        | <operable> / <operable>
        | <operable> % <operable>

    <window_spec> ::= <window>.over(<operable_list> [, <ordering_list>])

    <aggregate> ::=
        function.Count
        | function.Avg
        | function.Min
        | function.Max
        | function.Sum
        | ...

    <window> ::= <aggregate> | <ranking>

    <ranking> ::= function.RowNumber | ...



Examples
--------

.. code-block:: python

    from foobar.edu import schema  # our schema catalog

    school_ref = schema.School.reference('bar')
    QUERY = (
        schema.Student
        .inner_join(schema.Person, schema.Student.surname == schema.Person.surname)
        .inner_join(school_ref, schema.Student.school == school_ref.sid)
        .select(
            schema.Student.surname,
            school_ref['name'].alias('school'),
            function.Cast(schema.Student.score, dsl.Integer()).alias('score'),
        )
        .where(schema.Student.score > 0)
        .orderby(schema.Student.updated, schema.Student['surname'])
        .limit(10)
    )
