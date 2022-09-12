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

.. _registry:

Model Persistence
=================

During their :ref:`life cycles <lifecycle>`, ForML :ref:`projects <project>` produce specific
*artifacts* as their runtime deliverables. To store these artifacts, ForML uses :ref:`model
registry providers <registry-providers>` as the persistence layer managing the models at rest.

.. note::
   We use the term *model* more loosely including not just the involved *estimators* but
   essentially any :ref:`stateful actor <actor>` in the entire pipeline.

.. _registry-artifacts:

Project Artifacts
-----------------

The two types of artifacts requiring persistence are *code* in form of the :ref:`release package
<registry-package>` and *states* stored as :ref:`model generation assets <registry-assets>`.

The following diagram illustrates the logical hierarchy of the persistence layer based on a
particular instance of the ``posix`` registry provider holding a single :ref:`project <project>`
``forml-titanic-example`` with two :ref:`releases <lifecycle-release>` ``0.1.dev0`` and
``1.3.dev12`` - the first one having two :ref:`generations <lifecycle-generation>` with two
*model assets* each and the later just one generation with three model assets:


.. md-mermaid::

    flowchart LR
        subgraph reg1 ["Registry [posix]"]
            subgraph prj1 ["Project [forml-titanic-example]"]
                subgraph rel1 ["Release [0.1.dev0]"]
                    direction LR
                    pkg111[(package.4ml)]
                    subgraph gen1111 ["Generation [1]"]
                        direction TB
                        act11111[(asset1)]
                        act11112[(asset2)]
                        tag1111>tag.toml]
                    end
                    subgraph gen1112 ["Generation [2]"]
                        direction TB
                        act11121[(asset1)]
                        act11122[(asset2)]
                        tag1112>tag.toml]
                    end
                end
                subgraph rel2 ["Release [1.3.dev12]"]
                    direction LR
                    pkg112[(package.4ml)]
                    subgraph gen1121 ["Generation [1]"]
                        direction TB
                        act11211[(asset1)]
                        act11212[(asset2)]
                        act11213[(asset3)]
                        tag1121>tag.toml]
                    end
                end
            end
        end


.. _registry-package:

Release Package
^^^^^^^^^^^^^^^

The deployable project *code arrangement* produced :ref:`upon release <lifecycle-release>` from
within the :ref:`development life cycle <lifecycle-development>` is the binary :class:`ForML
package <forml.project.Package>`. It is a :doc:`zipfile object <python:library/zipfile>`
(typically a file with the ``.4ml`` suffix) containing all the project :ref:`principal components
<project-principal>` bundled together with all of its *runtime code dependencies* (as declared in
the :ref:`project setup <project-setup>`) plus some additional *metadata* (:class:`ForML package
manifest <forml.project.Manifest>`).

Each ForML package is published with an explicit version as specified in the :ref:`project setup
<project-setup>` at the time of release. All registry providers require packages of the same
project to have unique monotonically increasing version numbers.

.. _registry-staging:

Package Staging
"""""""""""""""

Registry providers might internally persist packages in an arbitrary format. In order to launch
their code using a :ref:`runner <runner>`, however, they need to be
:meth:`mounted <forml.io.asset.Registry.mount>` and exposed using a posix file system path known as
the *staging path* that is reachable from all runner nodes (for distributed deployment this implies
shared network posix file system).

.. _registry-assets:

Model Generation Assets
^^^^^^^^^^^^^^^^^^^^^^^

All :ref:`stateful actors <actor-type>` involved in a :ref:`project life cycle <lifecycle>`
require their internal state acquired during :ref:`training <workflow-mode>` to be persisted
using the model registry. States produced from the same training process represent the *model
generation assets* and every single follow-up training is leading to a new :ref:`generation
advancement <lifecycle-generation>`.

Generations are implicitly versioned using an integer sequence number starting from ``1``
(relatively to the same *release*) incremented upon every generation advancement.

Since each actor can implement an arbitrary way of :meth:`representing its own state
<forml.flow.Actor.get_state>`, the model assets are persisted as monolithic binary blobs with
a transparent structure.

The metadata associated with each generation is provided in form of an :class:`asset.Tag
<forml.io.asset.Tag>`.


.. _registry-management:

Content Management
------------------

Content of the registry can be managed using the :ref:`CLI <platform-cli>` as follows (see the
integrated help for full synopsis):

=========================  ===========================
Use case                   Command
=========================  ===========================
New release publishing     ``$ forml project release``
New generation publishing  ``$ forml model train``
Registry content listing   ``$ forml model list``
=========================  ===========================


Persistence API
---------------

Low-level
^^^^^^^^^

The low-level persistence interface is used mainly by the :ref:`registry providers
<registry-providers>`.

.. autoclass:: forml.project.Package

.. autoclass:: forml.project.Manifest

.. autoclass:: forml.io.asset.Tag

.. autoclass:: forml.io.asset.Registry
   :members: mount

High-level
^^^^^^^^^^

The following is the high-level persistence interface as used by the :ref:`runners <runner>`.

.. autoclass:: forml.io.asset.State

.. autoclass:: forml.io.asset.Instance

.. autoclass:: forml.io.asset.Directory

.. autoclass:: forml.io.asset.Project.Key
.. autoclass:: forml.io.asset.Release.Key
.. autoclass:: forml.io.asset.Generation.Key

.. _registry-providers:

Registry Providers
------------------

ForML comes with a number of :ref:`providers <provider>` implementing the
:class:`io.asset.Registry <forml.io.asset.Registry>` interface. To make them available
for the ForML runtime, selected providers need to be configured within the common :ref:`platform
setup <platform>` using the ``[REGISTRY.*]`` sections.

The official registry providers are:

.. autosummary::
   :template: provider.rst
   :nosignatures:

   forml.provider.registry.filesystem.volatile.Registry
   forml.provider.registry.filesystem.posix.Registry
   forml.provider.registry.mlflow.Registry
