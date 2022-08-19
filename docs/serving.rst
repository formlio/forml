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

.. _serving:

Serving Engine
==============

In addition to the basic :ref:`CLI-driven <platform-cli>` project-level batch-mode :ref:`execution
mechanism <platform-execution>`, ForML allows to operate the encompassing :ref:`applications
<application>` within an interactive loop performing the *apply* action of the :ref:`production
lifecycle <lifecycle-production>` - essentially providing *online predictions* a.k.a. *ML
inference* based on the underlying models.

.. _serving-process:

Process Control
---------------

The core component driving the serving loop is the *Engine*. To facilitate the end-to-end
prediction serving, it interacts with all the different :ref:`platform <platform>` sub-systems as
shown in the following sequence diagram:

.. md-mermaid::

    sequenceDiagram
        actor Client
        participant Engine as Engine/Gateway
        Client ->> Engine: query(Application, Request)
        opt if not in cache
            Engine ->> Inventory: get_descriptor(Application)
            Inventory --) Engine: Descriptor
        end
        Engine ->> Engine: Entry, Scope = Descriptor.receive(Request)
        Engine ->> Engine: ModelHandle = Descriptor.select(Scope)
        opt if needed for model selection
            Engine ->> Registry: inspect()
            Registry --) Engine: Metadata
        end
        Engine ->> Engine: Runner = get_or_spawn()
        Engine ->> Runner: apply(ModelHandle, Entry)
        opt if not loaded
            Runner ->> Registry: load(ModelHandle)
            Registry --) Runner: Model
        end
        opt if needs augmenting
            Runner ->> Feed: get_features(Entry)
            Feed --) Runner: Features
        end
        Runner ->> Runner: Outcome = Model.predict(Features)
        Runner --) Engine: Outcome
        Engine ->> Engine: Response = Descriptor.respond(Outcome)
        Engine --) Client: Response


This diagram illustrates the following steps:

#. Receiving a request containing the query payload and the target :ref:`application <application>`
   reference.
#. Upon a very first request for any given application, the engine fetches the particular
   :ref:`application descriptor <application-implementation>` from the configured :ref:`inventory
   <inventory>`. The descriptor remains cached for every follow-up request of that application.
#. The engine uses the descriptor of the selected application to :ref:`dispatch the request
   <application-dispatch>` by:

   #. :ref:`Interpreting <application-interpret>` the query payload.
   #. :ref:`Selecting <application-select>` a particular :ref:`model generation
      <registry-assets>` to serve the given request (depending on the model-selection strategy
      used by that application, this step might involve interaction with the :ref:`model registry
      <registry>`).

#. Unless already running, the engine spawns a dedicated :ref:`runner <runner>` which loads the
   selected :ref:`model artifacts <registry-artifacts>` providing an isolated environment not
   colliding with (dependencies of) other models also served by the same engine.
#. The runner might involve the configured :ref:`feed system <feed>` to augment the provided
   data points using a feature store.
#. With the complete feature-set matching the project defined :ref:`schema <project-source>`,
   the runner executes the :ref:`pipeline <project-pipeline>` in the :ref:`apply-mode
   <workflow-mode>` obtaining the prediction outcomes.
#. Finally, the engine again uses the application descriptor to :ref:`produce
   the response <application-interpret>` which is then returned to the original caller.

.. note::
    An engine can serve any :ref:`application <application>` available in its linked
    :ref:`inventory <inventory>` in a multiplexed fashion. Since the released :ref:`project
    packages <registry-package>` contain all the :ref:`declared dependencies <project-setup>`,
    the engine itself remains generic. To avoid collisions between dependencies of different
    models, the engine separates each one in an isolated context.


.. _serving-gateway:

Frontend Gateway
----------------

While the engine is full-featured in terms of the end-to-end application serving, it can only be
engaged using its raw Python API. That's suitable for products natively embedding the engine as
an integrated component, but for a truly decoupled client-server architecture this needs an extra
layer providing some sort of a transport protocol.

For this purpose, ForML comes with a concept of *serving frontend gateways*. They also follow the
:ref:`provider pattern <provider>` allowing to deliver number of different interchangeable
:ref:`implementations <serving-providers>` plugable at launch-time.

Frontend gateways represent the outermost layer in the logical hierarchy of the ForML architecture:

================================  =======================  =================  ====================
Layer                             Objective/Task           Problem question   Product/Instance
================================  =======================  =================  ====================
:ref:`Project <project>`          ML solution              How to solve?      Prediction outcomes
                                                                              (e.g. probabilities)
:ref:`Application <application>`  Domain interpretation,   How to utilize?    Domain response
                                  model selection                             (e.g. recommended
                                                                              products)
:ref:`Engine <serving>`           Serving control          How to operate?    Interactive
                                                                              processing loop
:ref:`Gateway <serving-gateway>`  Client-server transport  How to integrate?  ML service API
================================  =======================  =================  ====================

API
^^^

.. autoclass:: forml.runtime.Gateway
   :members: run

Service Management
^^^^^^^^^^^^^^^^^^

The gateway service can be managed using the :ref:`CLI <platform-cli>` as follows (see the
integrated help for full synopsis):

==========================  =============================
Use case                    Command
==========================  =============================
Launch the gateway service  ``$ forml application serve``
==========================  =============================


.. _serving-providers:

Gateway Providers
^^^^^^^^^^^^^^^^^

Gateway :ref:`providers <provider>` can be configured within the runtime :ref:`platform setup
<platform>` using the ``[GATEWAY.*]`` sections.

The available implementations are:

.. autosummary::
   :template: provider.rst
   :nosignatures:

   forml.provider.gateway.rest.Gateway
