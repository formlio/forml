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

.. autosummary::

   forml.lib.feed.static


Reader
------

.. autosummary::

   forml.lib.feed.reader.sql.alchemy
   forml.lib.feed.reader.sql.dbapi


API
---

.. autoclass:: forml.io.Feed
    :members:
