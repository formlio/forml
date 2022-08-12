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

.. _io:

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
    are handled by the :ref:`platform <platform>` (using the plugable :ref:`provider concept
    <provider>`) while :ref:`projects <project>` themselves are independent of any runtime
    configuration to truly offer a conceptual solution to the given problem - not just to its
    particular instance within some specific environment.

The concepts ForML uses to handle the data access are:

* :ref:`Schema Catalogs <io-catalog>` are the logical dataset representations distributed in
  catalogs available to both :ref:`projects <project>` and :ref:`platforms <platform>` as
  portable dataset references
* :ref:`Query DSL <dsl>` is the internal expression language used by projects to define high-level
  ETL operations on top of the *schema catalogs*
* :ref:`Source Feeds <feed>` are the platform data providers capable of resolving the
  project-defined DSL query using a particular data storage technology
* :ref:`Output Sinks <sink>` are the *feeds* counterparts responsible for handling the
  produced pipeline results
* :ref:`Model Registries <registry>` stand aside of the previous concepts as they deal with
  metadata rather than the data itself providing the pipeline state persistence


.. _io-catalog:

Schema Catalogs
---------------

To achieve the data access abstraction, ForML integrates the concept of *schema catalogs*.
Instead of implementing direct operations on specific data-source instances, projects use the
:ref:`DSL expression <dsl>` to define the input data ETL referring only to abstract data
:ref:`schemas <schema>`. It is then the responsibility of the platform :ref:`feeds <feed>` to
resolve the requested schemas (and the whole ETL queries specified on top) mapping them to the
actual data-sources hosted in the particular runtime environment.

A *schema catalog* is a logical collection of :ref:`schemas <schema>` which both -
:ref:`projects <project>` and :ref:`platforms <platform>` - can use as a mutual data reference. It
is neither a service nor a system, rather a passive set of namespaced descriptors implemented
simply as a *python package* that must be available to both the project expecting the particular
data and the platform supposed to serve that project.

.. _io-resolving:

Content Resolving
^^^^^^^^^^^^^^^^^

When a project workflow is submitted to any given platform, it attempts to resolve the schemas
referred in the :ref:`source query <io-source>` using the configured :ref:`feed providers
<feed>`, and only when all of these schema dependencies can be satisfied with the available data
sources, the platform is able to launch that workflow.

The following diagram demonstrates the trilateral relationship between *projects*, *schema
catalogs* and *platform feeds* - establishing the mechanism of the decoupled data access:

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
   :ref:`schema(s) <schema>` from within certain *catalogs* - here *Schema 1* from *Catalog 1*
   and *Schema 2* from *Catalog 2*.
#. This platform happens to be configured with three different :ref:`feed providers <feed>` capable
   of supplying (using its physical storage layer) four data-sources represented by the given
   *schema catalog* so that:

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


Publishing
^^^^^^^^^^

An obvious aspect of the schema catalogs is their *decentralization*. Since they are
implemented as python packages, they can be easily distributed using the standard means
for python package publishing. Currently, there is no naming convention for the schema definition
namespaces. Ideally, schemas should be published and held in namespaces of the original dataset
producers. For private first-party datasets (ie. internal company data) this is easy - the owner
would just maintain a package with schemas of their data sources. For public datasets, this
relies on some community-maintained schema catalogs like the :doc:`Openschema catalog
<openschema:index>`.

Continue to the :ref:`schema DSL <schema>` for more details regarding the actual implementation
and use-cases. Also refer to the mentioned :doc:`Openschema catalog <openschema:index>` for a real
instance of a ForML schema catalog.

.. _io-source:

Source Descriptor
-----------------

ForML projects specify their input data requirements - the :ref:`ETL query <dsl>` optionally
:ref:`composed <operator-composition>` with other transforming :ref:`operators
<operator>` - in form of a *source descriptor* (declared within the :ref:`source.py
<project-source>` project component).

This descriptor is created using the ``project.Source.query()`` class method:

.. autoclass:: forml.project.Source
   :members: extract, transform, Labels, query, bind


.. _io-payload:

Data Payloads
-------------

In line with the overall architecture, ForML is designed to be as much data format agnostic as
possible. Conceptually, there are several scopes involving payload exchange requiring compatibility
with the passing data.

Internal Payload Exchange
^^^^^^^^^^^^^^^^^^^^^^^^^

Payload-wise, the core ForML runtime is pretty generic, dealing only with few tiny interfaces to
handle the necessary exchange with absolutely minimal footprint. Following is the list of the
involved core payload types:

+------------------------+------------------------+------------------------------------------------+
| Producer Side          | Consumer Side          | Exchange Protocol                              |
+========================+========================+================================================+
| Origin data at rest    | Feed :class:`Reader    | Each *feed* acts as an adapter designed        |
|                        | <forml.io.Feed.Reader>`| specifically for the given origin format.      |
+------------------------+------------------------+------------------------------------------------+
| Feed :class:`Reader    | :ref:`Feed <feed>`     | Defined using the :class:`io.layout.Tabular    |
| <forml.io.Feed.Reader>`| Slicer                 | <forml.io.layout.Tabular>` interface.          |
+------------------------+------------------------+------------------------------------------------+
| :ref:`Feed <feed>`     | Project :ref:`Pipeline | Defined using the :class:`io.layout.RowMajor   |
| Slicer                 | <topology>`            | <forml.io.layout.RowMajor>` interface.         |
+------------------------+------------------------+------------------------------------------------+
| :ref:`Actor <actor>`   | :ref:`Actor <actor>`   | No specific format required, choice of         |
| Payload Output Port    | Payload Input Port     | mutually compatible actors is responsibility   |
|                        |                        | of the implementor, ForML only facilitates the |
|                        |                        | exchange (possibly subject to serializability).|
+------------------------+------------------------+------------------------------------------------+
| Project :ref:`Pipeline | Platform :ref:`Sink    | Defined using the :class:`io.layout.RowMajor   |
| <topology>`            | Writer <sink>`         | <forml.io.layout.RowMajor>` interface.         |
+------------------------+------------------------+------------------------------------------------+
| :ref:`Actor <actor>`   | :ref:`Model Registry   | Handled in form of a                           |
| State Output Port      | <registry>`            | :class:`bytestring <python:bytes>`             |
|                        |                        | as implemented by the :meth:`.get_state()      |
|                        |                        | <forml.flow.Actor.get_state>` method.          |
+------------------------+------------------------+------------------------------------------------+
| :ref:`Model Registry   | :ref:`Actor <actor>`   | Handled in form of a                           |
| <registry>`            | State Input Port       | :class:`bytestring <python:bytes>`             |
|                        |                        | as implemented by the :meth:`.set_state()      |
|                        |                        | <forml.flow.Actor.set_state>` method.          |
+------------------------+------------------------+------------------------------------------------+


.. autodata:: forml.io.layout.Native
.. autodata:: forml.io.layout.ColumnMajor
.. autodata:: forml.io.layout.RowMajor

.. autoclass:: forml.io.layout.Tabular
   :members: to_columns, to_rows, take_columns, take_rows


External Payload Exchange
^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to the core payloads, the :ref:`serving <serving>` layer involves few more data
exchanges using the following structures:

+------------------------+------------------------+------------------------------------------------+
| Producer Side          | Consumer Side          | Exchange Protocol                              |
+========================+========================+================================================+
| Application Client     | Serving :ref:`Gateway  | Each *gateway* acts as an adapter designed     |
|                        | <serving>`             | specifically for the given application protocol|
|                        |                        | handling the payload as a :class:`bytestring   |
|                        |                        | <python:bytes>` with an explicit               |
|                        |                        | :class:`Encoding <forml.io.layout.Encoding>`.  |
+------------------------+------------------------+------------------------------------------------+
| Serving :ref:`Gateway  | Serving :ref:`Engine   | Using the :class:`io.layout.Request            |
| <serving>`             | <serving>`             | <forml.io.layout.Request>` structure.          |
+------------------------+------------------------+------------------------------------------------+
| Serving :ref:`Engine   | Feed :class:`Reader    | Passing the decoded :class:`io.layout.Entry    |
| <serving>`             | <forml.io.Feed.Reader>`| <forml.io.layout.Entry>` to the given feed     |
|                        |                        | for potential augmentation.                    |
+------------------------+------------------------+------------------------------------------------+
| :ref:`Sink <sink>`     | Serving :ref:`Engine   | Using the :class:`io.layout.Outcome            |
| Writer                 | <serving>`             | <forml.io.layout.Outcome>` structure.          |
+------------------------+------------------------+------------------------------------------------+
| Serving :ref:`Engine   | Serving :ref:`Gateway  | Using the encoded :class:`io.layout.Response   |
| <serving>`             | <serving>`             | <forml.io.layout.Response>` structure.         |
+------------------------+------------------------+------------------------------------------------+
| Serving :ref:`Gateway  | Application Client     | Handling the payload as a                      |
| <serving>`             |                        | :class:`bytestring <python:bytes>` with an     |
|                        |                        | explicit :class:`Encoding                      |
|                        |                        | <forml.io.layout.Encoding>` wrapped to the     |
|                        |                        | given application protocol.                    |
+------------------------+------------------------+------------------------------------------------+

.. autoclass:: forml.io.layout.Entry

.. autoclass:: forml.io.layout.Outcome

.. autoclass:: forml.io.layout.Request
   :members: Decoded

.. autoclass:: forml.io.layout.Response

.. autoclass:: forml.io.layout.Stats


.. _io-encoding:

Payload Encoding
^^^^^^^^^^^^^^^^

ForML also depends on the following *encoding* features for the external payload exchange:

.. autoclass:: forml.io.layout.Encoding
   :members: header, parse, match

.. autoclass:: forml.io.layout.Encoder
   :members: encoding, dumps

.. autoclass:: forml.io.layout.Decoder
   :members: loads

.. autofunction:: forml.io.layout.get_encoder
.. autofunction:: forml.io.layout.get_decoder


Payload Transformation Operators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

ForML also provides bunch of payload transformation operators as part of the :mod:`pipeline library
<forml.pipeline.payload>`.
