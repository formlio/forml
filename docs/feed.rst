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

.. _feed:

Source Feed
===========

A *Feed* is a :ref:`runtime platform <platform>` component responsible for interpreting the
:ref:`ETL query <dsl>` defined as the :ref:`project source <project-source>` and :ref:`resolving
<io-resolution>` the requested dataset using the linked storage system.

Architecture
------------

The Feed concept is based on two main principles:

#. A DSL-interpreting :class:`Reader <forml.io.Feed.Reader>` acting as an adapter between the
   storage layer and the pipeline.
#. A :ref:`content resolver <io-resolution>` using an explicit mapping of the published
   :ref:`schema catalogs <io-catalog>` to the hosted data sources effectively matching the logical
   schemas with actual data.

Content resolution takes place in the scope of the :ref:`DSL parsing <query-parser>`
as part of the Reader routine simply by :class:`visiting <forml.io.dsl.parser.Visitor>` and
replacing the matched DSL :class:`Sources <forml.io.dsl.Source>`/:class:`Features
<forml.io.dsl.Feature>` with the mapped terms declared already using the parser-target semantic.

When launching the pipeline, ForML :ref:`runner <runner>` expands the Feed into one or more initial
tasks within the assembled :ref:`workflow <workflow>` making it a native part of the final
DAG to be executed.

The core Feed API looks as follows:

.. autodata:: forml.io.Producer

.. autoclass:: forml.io.Feed
   :members: producer, sources, features

.. autoclass:: forml.io.Feed.Reader
   :members: parser, format, read

For reference, several existing Reader implementations can be found under the
``forml.provider.feed.reader`` package:

.. autosummary::
   :nosignatures:

   forml.provider.feed.reader.alchemy.Reader


.. _feed-selection:

Contextual Feed Selection
-------------------------

Unlike the other :ref:`provider types <provider>` which explicitly nominate exactly one instance
each before launching, feeds go through a more dynamic process of selecting the most suitable
candidate in the context of the actual data query.

For this purpose, ForML uses the ``io.Importer`` class:

.. autoclass:: forml.io.Importer
   :members: match


.. _feed-setup:

Custom Feed Setup
-----------------

Existing generic Feed implementations can be :ref:`configured <platform-config>` as any other
:ref:`provider types <provider>`. The strong deployment-specific character of the
:ref:`content resolver <io-resolution>` setup (explicit mapping of the published :ref:`schemas
<io-catalog>` and the hosted data sources) might, however, require to declare *bespoke
Feed providers* using not just parametric configuration but rather directly as a non-generic code.

For more details, see the :ref:`custom provider <provider-custom>` setup instructions.


Feed Providers
--------------

Feed :ref:`providers <provider>` can be configured within the runtime :ref:`platform setup
<platform>` using the ``[FEED.*]`` sections.

The available implementations are:

.. autosummary::
   :template: provider.rst
   :nosignatures:

   forml.provider.feed.alchemy.Feed
   forml.provider.feed.monolite.Feed

.. rubric:: External Providers

+----------------------------------+-------------------------------------------------------------+
| :class:`openlake:openlake.Lite`  | ForML feed providing access to a number of public datasets. |
+----------------------------------+-------------------------------------------------------------+
