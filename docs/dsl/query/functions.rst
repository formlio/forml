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

.. _query-functions:

Functions and Operators
=======================

The DSL offers a number of built-in operators and functions to be used within the :ref:`query
expressions <query-syntax>` allowing to define a complex ETL process to be executed on the physical
data sources by the particular :ref:`feed <feed>` providers.

.. attention::
   The actual set of available functions is at this point rather limited focusing merely on
   the concept demonstration.


.. autosummary::
   :template: function.rst
   :toctree: ../../_auto

   forml.io.dsl.function._aggregate
   forml.io.dsl.function._comparison
   forml.io.dsl.function._conversion
   forml.io.dsl.function._datetime
   forml.io.dsl.function._logical
   forml.io.dsl.function._math
   forml.io.dsl.function._window
