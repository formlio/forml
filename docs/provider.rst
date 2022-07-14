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

Providers Library
=================

ForML providers are *plugins* implementing particular functionality defined within the framework
using an abstract interface to decouple itself from specific technologies allowing for a greater
operational flexibility.

Providers become available for runtime operations after being :ref:`properly configured
<platform-config>` within the given platform.


.. seealso::
    This page is merely a summarizing list of all the official providers shipped with ForML. API
    documentation as well as comprehensive description of their logical concepts is covered
    in individual chapters dedicated to each of the provider types respectively (linked in
    subsections below).


Model Registries
----------------

ForML delegates responsibility for :doc:`model persistence <registry>` to model registry providers
implementing the abstract :class:`forml.io.asset.Registry` base class.

.. autosummary::
   :template: provider.rst
   :nosignatures:
   :toctree: _auto

   forml.provider.registry.filesystem.volatile.Registry
   forml.provider.registry.filesystem.posix.Registry
   forml.provider.registry.mlflow.Registry


Runners
-------

Actual execution of the :doc:`ForML workflows <workflow/index>` is performed by the
:doc:`pipeline runner <runner>` providers implementing the :class:`forml.runtime.Runner` base class.

.. autosummary::
   :template: provider.rst
   :nosignatures:
   :toctree: _auto

   forml.provider.runner.dask.Runner
   forml.provider.runner.graphviz.Runner
   forml.provider.runner.pyfunc.Runner


Feeds
-----

To decouple projects from any physical data sources, ForML is using a generic :doc:`query DSL <dsl>`
working with logical schemas that only at runtime get resolved to actual data provided by the
platform-configured :doc:`set of feeds <feed>` implementing the :class:`forml.io.Feed` base class.

.. autosummary::
   :template: provider.rst
   :nosignatures:
   :toctree: _auto

   forml.provider.feed.static.Feed
   forml.provider.feed.alchemy.Feed
   openlake.Local


Sinks
-----

Reciprocally to the Feeds_ system, ForML is using :doc:`sink <sink>` providers for submitting the
:doc:`workflow <workflow/index>` results according to the particular implementation of the
:class:`forml.io.Sink` base class.

.. autosummary::
   :template: provider.rst
   :nosignatures:
   :toctree: _auto

   forml.provider.sink.stdout.Sink


Application Inventories
-----------------------

For managing the high-level :doc:`application descriptors <application>` driving the :doc:`serving
<serving>` layer, ForML defers to the :doc:`inventory <inventory>` providers implementing the
:class:`forml.io.asset.Inventory` base class.

.. autosummary::
   :template: provider.rst
   :nosignatures:
   :toctree: _auto

   forml.provider.inventory.posix.Inventory


Gateways
--------

The :doc:`serving layer <serving>` representing one of the possible :ref:`execution mechanisms
<platform-execution>` is using the gateway providers implementing the :class:`forml.runtime.Gateway`
base class.

.. autosummary::
   :template: provider.rst
   :nosignatures:
   :toctree: _auto

   forml.provider.gateway.rest.Gateway
