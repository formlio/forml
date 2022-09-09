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

.. _principles:

Principles
==========

This chapter provides a brief description of the main principles behind the ForML architecture.
Conceptually, they can be split into two main categories as presented in the following sections -
one dealing with the project implementation perspective and the other focusing on its operational
aspects.


Project Formalization
---------------------

Formalization is the prime concept ForML is built upon. Having a common *component structure* for
ML projects, an *expression API* for their workflows and a generic *data source DSL* describing the
required data inputs allows to programmatically handle the entire *project life cycle*. Moreover,
it leads to a cleaner implementation that is easier to maintain, extend or exchange between
different environments.

Project Component Structure
^^^^^^^^^^^^^^^^^^^^^^^^^^^

ForML introduces its lightweight convention for organizing machine learning projects on the
source code level. This is to lay down a structure that can be understood across projects and
allows (not only) ForML itself to interpret it programmatically.

Thorough description of the ForML project layout is described in the :ref:`project` chapter.

Data Source DSL
^^^^^^^^^^^^^^^

ForML comes with a robust DSL for specifying the data requirements of each project. This
allows to decouple ForML projects from any explicit data source formats and storages and only
refer to data using their abstract *schemas*. It is then job of the particular runtime
platform to feed the project pipeline with the actual data based on the given DSL query.

Full guide and the DSL reference can be found in the :ref:`dsl` chapter.

Workflow Expression API
^^^^^^^^^^^^^^^^^^^^^^^

ForML provides an elegant API for describing project workflows as expressions of *operator
compositions*. Workflow expressions transparently expand into a low-level acyclic task
dependency graph (DAG) of primitive *actors*. Based on the internal implementation of each of
the operators composed in the given expression, ForML is able to derive different task graphs
depending on the actual *life cycle phase*. This leaves the workflow definition very clean - with
all the main complexity carried out in lower layers.

More on the *Operator* and *Actor* concepts is discussed in the :ref:`workflow` chapters.

Life Cycle Handle
^^^^^^^^^^^^^^^^^

Thanks to the formal project setup, ForML is inherently able to handle all of the defined project
*life cycle* actions right out of the box.

For more details, please see the :ref:`lifecycle` chapter.


Runtime Independence
--------------------

ForML has been carefully designed to entirely abstract away all of the fundamental runtime
dependencies so that project implementations stay decoupled from any particular execution mechanism,
storage technology, or data source format. This allows running the same unchanged project against an
arbitrary combination of these runtime *providers*. Specific providers are selected via the
configuration of the runtime environment called simply the :ref:`ForML platform <platform>`.


Data Input & Output
^^^^^^^^^^^^^^^^^^^

The project-defined data source DSL query gets translated into a reader-specific ETL code and
interpreted by one of the available schema-matching *feed* providers. Feeds can potentially
serve an arbitrary number of data sources that are advertised against their representative *schema
catalogs* also referenced from projects. A platform can be preconfigured with multiple different
feeds held in a *pool* which at query time selects a feed that is most suitable for the given
project query.

Similarly, any output produced by ForML pipelines gets captured by the platform and sent
to a configured *sink*.

See the :ref:`io` chapter for more information about the related concepts.

Persistence
^^^^^^^^^^^

Life cycle iterations of ForML projects depend on external persistence of their two
main *artifact* types - the particular version of the project *code* and the collection of its
internal *states* acquired during training/tuning. ForML platform automatically handles this
persistence via its supported *registry* providers.

See the :ref:`registry` chapter for the complete description of the persistence layer as well
as the list of the available registry implementations.

Execution
^^^^^^^^^

At runtime, the native actor DAG produced through the operator composition gets transcoded to
the specific representation of the selected third-party task dependency *runner* and the actual
execution is carried under its control.

A list of the supported runner providers as well as further details on this topic can be
found in the :ref:`runner` chapter.

Serving
^^^^^^^

Specific high-level extension of the execution principle is the *serving layer* allowing to expose
the published models for online inference. ForML defers to its *application gateway* providers to
implement different possible serving interfaces configured as part of the runtime platform.

See the :ref:`serving` chapter for more details.
