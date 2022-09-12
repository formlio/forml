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

.. _inventory:

Application Inventory
=====================

To expose :ref:`applications <application>` for serving, ForML uses the application inventory as
a lightweight storage service used for application deployment. At runtime, the :ref:`serving
engine <serving>` looks up the requested :ref:`descriptors <application-implementation>` of
published applications to facilitate their serving.

In order to provide the inventory service, particular inventory :ref:`provider <provider>`
instance(s) need to be configured within the :ref:`runtime platform setup <platform>`.


.. _inventory-management:

Content Management
------------------

Content of the inventory is populated through :ref:`application publishing <application-publishing>`
and can be managed using the :ref:`CLI <platform-cli>` as follows (see the integrated help for
full synopsis):

==========================  ============================
Use case                    Command
==========================  ============================
Application (re)publishing  ``$ forml application put``
Inventory content listing   ``$ forml application list``
==========================  ============================


API
---

The inventory concept is based on the following simple API:

.. autoclass:: forml.io.asset.Inventory
    :members: list, get, put

.. autoclass:: forml.application.Descriptor.Handle


Inventory Providers
-------------------

Inventory :ref:`providers <provider>` can be configured within the runtime :ref:`platform setup
<platform>` using the ``[INVENTORY.*]`` sections.

The available implementations are:

.. autosummary::
   :template: provider.rst
   :nosignatures:

   forml.provider.inventory.posix.Inventory
