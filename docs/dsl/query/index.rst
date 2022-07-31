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

.. _query:

Query Statement
===============

The query DSL is a robust API for describing the ETL process of deriving datasets specified by
ForML :ref:`projects <project>` from origin data-sources represented by their logical :ref:`schemas
<schema>`.

While constructing the DSL query statements, the API internally builds up a generic model of the
required ETL process. The query has purely descriptive character, there is no native
mechanism of its direct execution. Instead, it is expected to be :ref:`parsed <query-parser>`
at runtime into a set of instructions corresponding to the selected :ref:`feed <feed>` and its
target storage layer hosting the physical data-sources :ref:`matching <io-resolving>` the requested
schemas.

Following is the list of the individual chapters covering this topic in detail:

.. toctree::
    :maxdepth: 2

    syntax
    functions
    design
