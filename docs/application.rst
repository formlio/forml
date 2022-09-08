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

.. _application:

Application
===========

ForML applications represent a high-level concept built on top of ForML :ref:`projects <project>`
as deliverables for the :ref:`serving layer <serving>`.

The term *application* in this context doesn't hold the common meaning as a general computer program
covering a wide range of potential functions. ForML applications obviously focus just on the *ML
inference* (providing predictions in response to the presented data points) representing the *apply*
stage of the :ref:`production lifecycle <lifecycle-production>`.

While the purpose of :ref:`projects <project>` is to implement a *portable solution* to the given
ML problem, *applications*  aim to expose it (by means of :ref:`gateway providers
<serving-gateway>`) in a domain-specific form suitable for integration with the *actual* decision
making process.

ForML :ref:`platform <platform>` persists :ref:`published applications <application-publishing>`
within a special :ref:`application inventory <inventory>` where they are picked from at runtime
by the :ref:`serving engine <serving>`.


.. _application-prjrelation:
.. rubric:: Project-Application Relationship

As shown in the diagram below, relationships between projects and applications can have any
possible cardinality. Projects might not be associated with any application (not exposed for
serving - e.g. *Project B*), on the other hand an application can possibly span multiple
(compatible) projects (its :ref:`model selection strategy <application-select>` can involve
multiple projects - e.g. *Application Y*) or a single project might be utilized by several
different applications (e.g. *Project A*).

.. md-mermaid::

    flowchart LR
        subgraph registry ["Registry"]
            subgraph prja ["Project A"]
                gena1[("Generation 1")]
                gena2[("Generation 2")]
            end
            subgraph prjb ["Project B"]
                genb1[("Generation 1")]
            end
            subgraph prjc ["Project C"]
                genc1[("Generation 1")]
            end
        end
        subgraph inventory ["Inventory"]
            app1(["Application X"]) --- gena1 & gena2
            app2(["Application Y"]) --- gena2 & genc1
        end
        subgraph gw ["Gateway"]
            eng["Engine"] --- app1 & app2
        end

It makes sense to manage an application (descriptor) in the scope of some particular project if
they form a 1:1 relationship (perhaps the most typical scenario). More complex applications might
need to be maintained separately though.

.. _application-dispatch:

Request Dispatching
-------------------

Applications play a key role in the :ref:`serving process <serving-process>` taking control over the
following steps:

.. md-mermaid::

    sequenceDiagram
        Engine ->> Application: receive(Request)
        Application --) Engine: Entry, Scope
        Engine ->> Application: select(Scope, Stats)
        Application --) Engine: Model
        Engine ->> Model: predict(Entry)
        Model --) Engine: Outcome
        Engine ->> Application: respond(Outcome, Scope)
        Application --) Engine: Response


.. _application-interpret:

Data Interpretation
^^^^^^^^^^^^^^^^^^^

Applications define how to understand the received query and how to turn it into a
model-prediction request, as well as how to present the predicted outcomes as the
domain-specific response.

This is implemented within the following steps:

#. Formally :meth:`receiving <forml.application.Descriptor.receive>` the query by:

   #. :ref:`Decoding <io-encoding>` its content according to the implemented payload semantic.
      Applications might chose to support number of different encodings.
   #. Optionally compile the query into prediction-relevant data points. This might possibly
      involve certain *domain mapping* (e.g. a recommender application receiving click-stream
      events would turn it at this point into a set of product features to be passed down
      for scoring, etc.).
   #. Optionally assemble custom metadata to constitute an application *context* to be carried
      through the serving layers for reference.

#. Producing a :meth:`response <forml.application.Descriptor.respond>` based on:

   #. Composing the domain-specific result message out of the prediction outcomes (generated
      by the engine using the :meth:`selected <forml.application.Descriptor.select>` model).
      This might again involve particular domain mapping (e.g. turning probabilities into a
      selection of products, etc.).
   #. :ref:`Encoding <io-encoding>` the response payload into a client-accepted representation.


.. _application-select:

Model Selection
^^^^^^^^^^^^^^^

Another powerful way an application exerts control over the serving process is a dynamic
:meth:`selection <forml.application.Descriptor.select>` of the specific :ref:`model generation
<registry-assets>` to be used for serving each particular request.

Applications can base the selection logic on the following available facts:

* actual content of the :ref:`model registry <registry>` (any existing model generation to choose
  from)
* custom metadata stored in the application *context* (e.g. as part of the query :meth:`receiving
  <forml.application.Descriptor.receive>`)
* various serving :class:`metrics <forml.runtime.Stats>` provided by the system
  (e.g. number of requests already served by this application - using which model - including
  actual :ref:`performance tracking <evaluation-perftrack>` results of each of the models,
  etc.)

The model-selection mechanism allows to implement complex serving strategies including *A/B
testing*, *multi-armed bandits*, *cold-start*/*fallback* models, etc. It is due to this dynamic
ability to select a particular model/generation on the fly why the project-application
relationship can potentially have higher than just the ordinary 1:1 cardinality.

.. _application-implementation:

Implementation
--------------

Similarly to the :ref:`principal project components <project-principal>`, applications are
delivered in form of a python module (single file with the ``.py`` suffix) providing an
implementation of the ``application.Descriptor``:

.. autoclass:: forml.application.Descriptor
   :members: name, receive, respond, select

.. caution::
    Unlike projects, which upon :ref:`releasing <lifecycle-release>` produce a :ref:`ForML package
    <registry-package>` containing all of their runtime dependencies, application descriptors are
    :ref:`published <inventory>` as-is without any implicit dependency management. Any such
    dependencies would need to be satisfied explicitly by the general runtime environment (given the
    application scope, the dependencies are expected to be rather lightweight though).


The descriptor needs to be registered within the delivering module via a call to the
``application.setup()`` function:

.. autofunction:: forml.application.setup


.. _application-generic:

Generic Application
^^^^^^^^^^^^^^^^^^^

In addition to the abstract :class:`application.Descriptor <forml.application.Descriptor>`, ForML
for convenience also provides a generic out-of-the-box implementation suitable for most typical
scenarios.

This implements the :ref:`data interpretation <application-interpret>` simply using the
available :func:`layout.get_decoder <forml.io.layout.get_decoder>` and :func:`layout.get_encoder
<forml.io.layout.get_encoder>` codecs and for the :ref:`model selection <application-select>`
it introduces a concept of plugable :class:`application.Selector <forml.application.Selector>`
strategies.

.. autoclass:: forml.application.Generic
   :show-inheritance:

Generic applications are configured with particular model selection strategies provided as
implementations of the following ``application.Selector`` base class:

.. autoclass:: forml.application.Selector
   :members: select


.. _application-strategy:

Strategies
""""""""""

Following are available implementations of model selection strategies to be used when configuring
any :ref:`generic application <application-generic>`.

.. autoclass:: forml.application.Explicit
   :show-inheritance:

.. autoclass:: forml.application.Latest
   :show-inheritance:


.. _application-publishing:

Publishing
----------

Applications get deployed by publishing into an :ref:`application inventory <inventory>` used by
the particular :ref:`serving engine <serving>`. Unlike the :ref:`project artifacts
<registry-artifacts>`, applications are not versioned and are only held in a flat namespace
depending on uniqueness of each application :meth:`name <forml.application.Descriptor.name>`.
(Re)publishing an application with existing name overwrites the original instance.


.. code-block:: console

    $ forml application put titanic.py
    $ forml application list
    forml-tutorial-titanic


.. note::
    The name of the *module* containing the application descriptor is (from the publishing
    perspective) functionally meaningless. The only relevant identifier is the :meth:`application
    name <forml.application.Descriptor.name>`.
