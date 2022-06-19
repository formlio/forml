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

Model Persistence
=================

There is a couple of *artifacts* derived from a typical ForML :doc:`project <project>` at
certain stage of its :doc:`lifecycles <lifecycle>` as the runtime deliverables. ForML uses
the *model registry* to persist these artifacts providing an interface for their follow-up
management.

.. note::
   We use the term *model* more loosely including not just the involved *estimators* and/or
   *regressors* but essentially any :doc:`stateful actor <workflow/actor>` in the pipeline.

.. _registry-artifacts:

Project Artifacts
-----------------

The two types of artifacts are code and states

.. _registry-package:

Release Package
^^^^^^^^^^^^^^^

The deployable project representation produced :ref:`upon releasing <lifecycle-release>` from
within the :ref:`development lifecycle <lifecycle-development>` is the binary *ForML package*.
It is a single monolithic object (typically a file with the ``.4ml`` suffix) containing all the
project *principal components* bundled together with some *metadata* (in form of a ForML
manifest) and especially all of its *runtime code dependencies* (as declared in the :ref:`project
setup <project-setup>`).

has unique version

.. autoclass:: forml.project.Package

Published as part of the release into :doc:`registry`.

.. _registry-assets:

Model Generation Assets
^^^^^^^^^^^^^^^^^^^^^^^

has incremental id

binary byte blobs

:ref:`production lifecycle <lifecycle-production>` upon training/tuning


Model Registry Providers
------------------------

implementing the abstract ``io.asset.Registry``

Configured as per :doc:`platform`.

ForML can use multiple registries built upon different technologies. The available registry implementations are:

.. autosummary::

   forml.provider.registry.filesystem.posix
   forml.provider.registry.filesystem.volatile
   forml.provider.registry.mlflow
