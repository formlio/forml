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

IO Concept
==========

ForML comes with a unique approach to data access. For projects to be truly portable, they must
not be coupled directly with any specific data storages or formats. Accessing an explicit
instance of a dataset as well as interpretation of its particular structure can't be part of the
ML solution implemented as a ForML project. Instead, the solution has to work with an abstract
representation of the relevant logical dataset, which only at runtime gets resolved using its
actual provider (if available).

.. important::
    Within the ForML architecture, all runtime system dependencies - including the pipeline I/O -
    are handled by the :doc:`platform <platform>` (using the plugable :doc:`provider concept
    <provider>`) while :doc:`projects <project>` themselves are independent of any runtime
    configuration to truly offer a conceptual solution to the given problem - not just to its
    particular instance within some specific environment.

The concepts ForML uses to handle the data access are:

* :ref:`Catalogued Schemas <io-schemas>` are the logical dataset representations distributed in
  catalogs available to both :doc:`projects <project>` and :doc:`platforms <platform>` as
  portable dataset references
* :doc:`Query DSL <dsl>` is the internal expression language used by projects to define high-level
  ETL operations on top of the *catalogued schemas*
* :doc:`Source Feeds <feed>` are the platform data providers capable of resolving the
  project-defined DSL query using a particular data storage technology
* :doc:`Output Sinks <sink>` are the *feeds* counterparts responsible for handling the
  produced pipeline results
* :doc:`Model Registries <registry>` stand aside of the previous concepts as they deal with
  metadata rather than the data itself providing the pipeline state persistence


.. _io-schemas:

Catalogued Schemas
------------------

To achieve the data access abstraction, ForML integrates the concept of *catalogued schemas*.
Instead of implementing direct operations on specific data-source instances, projects use the
:doc:`DSL expression <dsl>` to define the input data ETL referring only to abstract data
:ref:`schemas <dsl-schema>`. It is then the responsibility of the platform :doc:`feeds <feed>` to
resolve the requested schemas (and the whole ETL queries specified on top) mapping them to the
actual data-sources hosted in the particular runtime environment.

A *schema catalog* is a logical collection of :ref:`schemas <dsl-schema>` which both -
:doc:`projects <project>` and :doc:`platforms <platform>` - can use as a mutual data reference. It
is neither a service nor a system, rather a passive set of namespaced descriptors implemented
simply as a python package that must be available to both the project expecting the particular
data and the platform supposed to serve that project. When a project workflow is submitted to any
given platform, it attempts to resolve the schemas referred in the :ref:`source query
<io-source>` using the configured :doc:`feed providers <feed>`, and only when all of these schema
dependencies can be satisfied with the available data sources, the platform is able to launch
that workflow.

The following diagram demonstrates the trilateral relationship between *projects*, *schema
catalogues* and *platform feeds* - establishing the mechanism of the decoupled data access:

.. md-mermaid::

    flowchart TB
        subgraph Schema Catalog 1
            s1{{Schema 1}}
        end
        subgraph Schema Catalog 2
            direction LR
            s3{{Schema 3}}
            s2{{Schema 2}}
            s4{{Schema 4}}
        end
        subgraph project [Project]
            src[[source.py]] --> s1 & s2
            pip[[pipeline.py]]
            eval[[evaluation.py]]
        end
        subgraph platform [Runtime Platform]
            subgraph Storage Layer
                direction LR
                db[(Database)]
                xy[(Other)]
                fs[(Filesystem)]
            end
            subgraph Feed Providers
                direction LR
                xyf[/Other Feed/]
                dbf[/DB Feed/]
                fsf[/FS Feed/]
                xyf ---> s1 & s2
                xyf --> xy
                fsf --> fs
                fsf ---> s4
                dbf ---> s3 & s4
                dbf --> db
            end
        end
        project ===> platform

It tells the following story:

#. A *project* defines its data requirements using a :ref:`source query <io-source>` specified in
   its :ref:`source.py component <project-source>` referring to particular data-source
   :ref:`schema(s) <dsl-schema>` from within certain *catalogues* - here *Schema 1* from
   *Catalogue 1* and *Schema 2* from *Catalogue2*.
#. This platform happens to be configured with three different :doc:`feed providers <feed>` capable
   of supplying (using its physical storage layer) four data-sources represented by the given
   *catalogued schemas* so that:

   * the *DB Feed* can serve data-sources represented by *Schema 3* and *Schema 4* physically stored
     in the *Database*
   * the *FS Feed* can also provide the data-source matching the *Schema 4* duplicating its
     physical copy stored on the *Filesystem*
   * finally the *Other Feed* knows how to supply data for schema *Schema 1* and *Schema 2*

#. When the project gets :ref:`launched <platform-execution>` on the platform, its :ref:`source
   descriptor <io-source>` first goes through the :ref:`feed selection <feed-selection>` process to
   find the most suitable feed provider for the given query, followed by actual execution of the
   particular query by that selected feed, which results in :ref:`data payload <io-payload>`
   entering the project workflow.

An obvious aspect of the schema catalogs is their *decentralization*. Currently, there is no
naming convention for the schema definition namespaces. Ideally, schemas should be published and
held in namespaces of the original dataset producers. For private first-party datasets (ie. internal
company data) this is easy - the owner would just maintain a (private) package with schemas of
their data sources. For public datasets, this relies on some community-maintained schema catalogs
like the :doc:`Openschema catalog <openschema:index>`.

Continue to the :ref:`schema DSL <dsl-schema>` for more details regarding the actual implementation
and use-cases.

.. _io-source:

Source Descriptor
-----------------

ForML projects specify their input data requirements - the :doc:`ETL query <dsl>` optionally
:ref:`composed <operator-composition>` with other transforming :doc:`operators
<workflow/operator>` - in form of a *source descriptor* (declared within the :ref:`source.py
<project-source>` project component).

This descriptor is created using the ``project.Source.query()`` class method:

.. autoclass:: forml.project.Source
   :members: extract, transform, Labels, query, bind


.. _io-payload:

Payloads
--------

agnostic, compatibility is users choice/responsibility

.. autoclass:: forml.io.layout.Tabular


ToPandas
