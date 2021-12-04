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

Model Registry
==============

At :doc:`runtime <platform>`, the :ref:`production lifecycle <lifecycle-production>` uses the model registry for storing
:ref:`artifacts <concept-persistence>` of project *lineages* as well as the models of its *generations*.

ForML can use multiple registries built upon different technologies. The available registry implementations are:


.. autosummary::

   forml.lib.registry.filesystem.posix
   forml.lib.registry.filesystem.virtual


API
---

.. autoclass:: forml.runtime.asset.Registry
    :members:


Providers
---------

.. toctree::
    :maxdepth: 2

    filesystem
