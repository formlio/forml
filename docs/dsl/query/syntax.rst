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

TODO: explain SQL resemblance


The DSL allows to specify a rich ETL procedure of retrieving the data in any required shape or form. This can be
achieved through the *query* API that's available on top of any :class:`dsl.Schema
<forml.io.dsl.Schema>` object. Important feature of the query syntax is also the support for
column `expressions`_.


Following is the list of the query API methods:

Example query might look like::

    ETL = student.join(person, student.surname == person.surname)
            .join(school_ref, student.school == school_ref.sid)
            .select(student.surname.alias('student'), school_ref['name'], function.Cast(student.score, kind.String()))
            .where(student.score < 2)
            .orderby(student.level, student.score)
            .limit(10)

Expressions
-----------

Any schema field representing a data column can be involved in a *column expression*. All the schema field objects
implement number native of operators, that can be used to directly form an expression. Furthermore, there are separate
function modules that can be imported to build more complex expressions.

The native operators available directly on the field instances are:

+--------------+-----------------------------------------------------------+
| Type         | Syntax                                                    |
+==============+===========================================================+
| Alias        | .. automethod:: forml.io.dsl.Operable.alias               |
+--------------+-----------------------------------------------------------+
