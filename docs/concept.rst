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

Concept
=======

ForML aims to address a wide spectrum of challenges emerging from classical ML projects. It combines a unique set of
features to help with all of the project phases starting from research and prototyping to delivery and beyond.

The following table presents a quick overview of the key features brought by ForML:

+----------------------------------+---------------------------------------------------------------------------+
| Merit                            | Contributing factor                                                       |
+==================================+===========================================================================+
| *flexibility*, *agility*         | iterative development, continuous experimentation, optional interactivity |
+----------------------------------+---------------------------------------------------------------------------+
| *unification*, *reusability*     | project structure convention, high-level workflow API                     |
+----------------------------------+---------------------------------------------------------------------------+
| *reproducibility*, *consistency* | versioned pipeline artifacts, native operator modality                    |
+----------------------------------+---------------------------------------------------------------------------+
| *portability*, *operability*     | multilevel abstraction, pluggable provisioning, runtime independence      |
+----------------------------------+---------------------------------------------------------------------------+

Conceptually these features can be split into two domains as presented in the next sections. The first one is
approaching a project from its implementation perspective and the other is dealing with its operational aspects.


Project Formalization
---------------------

Formalization is the prime concept ForML is built upon. Having a common *component structure* for ML projects,
an *expression API* for their workflows and generic *DSL* describing the required data sources leads to a cleaner
implementation that is easier to maintain, extend or exchange between different environments.

.. _concept-project:

Project Component Structure
    ForML introduces its convention for organizing machine learning projects on the module level. This is to have
    some common basic structure that can be understood across projects and help (not only) ForML itself to understand
    the project just by visiting the expected places.

    More details about the project layout are explained in the :doc:`project` sections.

.. _concept-dsl:

Data Source DSL
    ForML comes with custom DSL for specifying the data sources required by the project. This allows to decouple the
    project from particular data formats and storages and only refer to it using *catalogized schemas*. It is then
    down to the particular execution platform to feed the project pipeline with the actual data based on the given
    DSL query.

    Example of data source DSL::

        student.join(person, student.surname == person.surname) \
                .join(school, student.school == school.sid) \
                .select(student.surname.alias('student'), school['name'], function.Cast(student.score, kind.String())) \
                .where(student.score < 2) \
                .orderby(student.level, student.score)

    Full guide and the DSL references can be found in the :doc:`dsl` sections.

.. _concept-workflow:

Workflow Expression API
    ForML provides a convenient interface for describing the project workflow using high-level expressions for
    *operator compositions* that transparently expand into a low-level acyclic task dependency graph (DAG) of primitive
    *actors*. Based on the internal architecture of operators, ForML is able to derive different DAG shapes from the
    same workflow depending on the actual lifecycle phase implementing the requested pipeline mode. This leaves the
    workflow definition very clean with all the main complexity being carried out in lower layers.

    Example of simple workflow::

        FLOW = SimpleImputer(strategy='mean') >> LogisticRegression(max_iter=3, solver='lbfgs')

    More on the *Operators* and *Actors* is discussed in the :doc:`workflow/index` sections. See
    also the :doc:`lifecycle` sections for details on the supported pipeline modes.


Runtime Independence
--------------------

ForML has been carefully designed to entirely abstract away all of the fundamental runtime dependencies so that project
implementation is decoupled from any particular execution mechanism, storage technology, or data format. This allows
running the same unchanged project against an arbitrary combination of these runtime *providers*. Specific providers are
selected by the configuration of the runtime environment called simply the :doc:`Platform <platform>`.

.. _concept-io:

Data Providers & Result Consumers
    The data source DSL defined within the project gets transcoded into a reader-specific ETL code and then served
    by one of the available schema-matching *feed* providers. Feeds can potentially serve an arbitrary number of
    data sources that are advertised against the same *schema catalogs* referred by projects. A platform can be
    preconfigured with multiple different feeds held in a *pool* which at query time selects the most suitable feed to
    serve the given project query.

    Similarly, any output produced by the ForML pipeline gets captured by the platform and sent to a configured *sink*.
    A platform can specify different sink provider for each pipeline mode.

    See the :doc:`io` sections for more info about the related concepts.

.. _concept-persistence:

Persistence
    A fundamental aspect of a project lifecycle is the pipeline state transition occurring during *train* and/or *tune*
    modes. Each of these transitions produces a new *Generation*. Generations based on the same build of a project
    belong to one *Release*.

    Both Releases and Generations are *project artifacts* that require persistent runtime storage called *Registry*
    that allows publishing, locating and fetching these entities. See the :doc:`registry` section for the list of
    existing registry implementations and their configurations.

.. _concept-execution:

Execution
    At runtime, the native actor DAG produced through the operator composition gets transformed to a representation
    of the selected third-party task dependency *runner* and the actual execution is carried under its control.

    The list of supported runners shipped with ForML and their documentation can be found in the :doc:`runner`
    section.
