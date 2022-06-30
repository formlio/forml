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

TODO: decoupled from any specific technology...

ForML uses an internal *bank* of available provider implementations of the different possible types.
Provider instances are registered in this bank using one of two possible *references*:

* provider's *fully qualified class name* - for example, the ``forml.provider.runner.dask:Runner``
* for convenience, each provider can also optionally have an *alias* defined by its author - ie
  ``dask``

.. caution::
   For any provider implementation to be placed into the ForML provider bank, it needs to get
   imported somehow. When the bank is queried for any provider instance using its reference, it
   either is matched and returned or ForML attempts to import it. If it is queried using the
   fully qualified class name, it is clear where to import it from (assuming the module is on
   :data:`python:sys.path`). If it is however referenced by the alias, ForML only considers
   providers from the main library shipped with ForML. This means external providers cannot be
   referenced using their aliases as ForML has no chance knowing where to import them from.


Model Registries
----------------

Base: :class:`forml.io.asset.Registry`

.. autosummary::
   :template: provider.rst
   :nosignatures:
   :toctree: _auto

   forml.provider.registry.filesystem.volatile.Registry
   forml.provider.registry.filesystem.posix.Registry
   forml.provider.registry.mlflow.Registry


Runners
-------

Base: :class:`forml.runtime.Runner`

.. autosummary::
   :template: provider.rst
   :nosignatures:
   :toctree: _auto

   forml.provider.runner.dask.Runner
   forml.provider.runner.graphviz.Runner
   forml.provider.runner.pyfunc.Runner


Application Inventories
-----------------------

Base: :class:`forml.io.asset.Inventory`

.. autosummary::
   :template: provider.rst
   :nosignatures:
   :toctree: _auto

   forml.provider.inventory.posix.Inventory

Feeds
-----

Base: :class:`forml.io.Feed`

.. autosummary::
   :template: provider.rst
   :nosignatures:
   :toctree: _auto

   forml.provider.feed.static.Feed

Sinks
-----

Base: :class:`forml.io.Sink`

.. autosummary::
   :template: provider.rst
   :nosignatures:
   :toctree: _auto

   forml.provider.sink.stdout.Sink

Gateways
--------

Base: :class:`forml.runtime.Gateway`

.. autosummary::
   :template: provider.rst
   :nosignatures:
   :toctree: _auto

   forml.provider.gateway.rest.Gateway
