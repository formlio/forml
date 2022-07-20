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

.. _dsl:

Data Source DSL
===============

To allow :ref:`projects to declare <project-source>` their data requirements in a portable way
independently of any particular storage technology and data formats, ForML comes with its custom
*Data Source DSL* (domain-specific language) which gets interpreted at :ref:`runtime
<platform-execution>` by the :ref:`feeds subsystem <feed>` performing the :ref:`content resolution
<io-resolving>` routine to deliver the requested datasets.

Conceptually, it is an *internal* DSL (i.e. within Python grammar) based on *declarative* style of
specifying the data profiles using the following two main constructs:

* :ref:`schema definition <schema>` syntax for logical representation of individual datasets
* :ref:`query statement <query>` notation for declaring the project data requirements

.. important::
    Do not confuse the DSL with an ORM framework. The DSL entities are not used to manage any data
    directly. Its sole purpose is to describe the data sources independently of data access
    mechanism in the same way the :ref:`workflow expression <workflow-expression>` describe the
    processing logic independently of the execution mechanism. In both cases, the abstract
    descriptions get *transcoded* to runtime-specific instructions when launched.


The DSL agenda is divided into the following chapters:

.. toctree::
    :maxdepth: 1

    schema
    query/index
