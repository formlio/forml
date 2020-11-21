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

Data Source DSL
===============

To allow projects to :ref:`specify <project-source>` their data requirements in a portable way, ForML comes with its
generic DSL that's at :doc:`runtime <../platform>` interpreted by the :doc:`feeds subsystem <../feed>`.

Schema
------



Query
^^^^^

.. autoclass:: forml.io.dsl.struct.frame.Query
    :members: columns, select, join, groupby, having, where, limit, orderby


Functions
---------

.. autosummary::
   :recursive:
   :toctree: _auto

   forml.io.dsl.function.aggregate
   forml.io.dsl.function.conversion
   forml.io.dsl.function.datetime
   forml.io.dsl.function.math
