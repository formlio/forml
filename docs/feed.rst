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

Source Feed
===========

Feed is a :doc:`runtime platform <platform>` component responsible for resolving the :doc:`project defined <project>`
:doc:`ETL query <dsl>` providing the requested data.


Feed Providers
--------------

Among the different *provider* types, :doc:`Feeds <feed>` are unique as each instance usually needs to be special
implementation specific to the given platform. Part of the feed functionality is to resolve the :ref:`catalogized
schemas <io-catalogized-schemas>` to the physical datasets known to the platform. This might not be always possible via
configuration and the whole feed needs to be implemented as code. For this purpose, the *system* and *user*
configuration directories are also potentially searched by the provider importer so that the custom feeds can be placed
there.

For the special case of the public datasets described using the :doc:`Openschema catalog<openschema:index>`, there is a
lightweight feed provided in form of the installable :doc:`Openlake package<openlake:install>`.


.. autosummary::
   :template: provider.rst
   :nosignatures:

   forml.provider.feed.static.Feed
   openlake.Local


Reader
------

.. autosummary::

   forml.provider.feed.reader.sql.alchemy
   forml.provider.feed.reader.sql.dbapi


API
---

.. autoclass:: forml.io.Feed
   :members:
