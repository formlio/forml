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

.. _sink:

Output Sink
===========

Sink is a :ref:`runtime platform <platform>` component responsible for processing the output
produced upon executing one of the :ref:`lifecycle actions<lifecycle-actions>` of the particular
:ref:`project pipeline <project-pipeline>`.

It is a (much simpler) logical counterpart to the :ref:`feed concept <feed>`.

Individual sink providers are mostly relevant to :ref:`batch mode <platform-execution>`. The concept
is still used also in the :ref:`serving mode <serving>`, but the component is embedded in the engine
which transparently deals with the output.

Architecture
------------

From the high-level perspective, Sink mirrors the :ref:`feed design <feed>` with flow inversion.
It relies on a particular :class:`Writer <forml.io.Writer>` implementation acting as an adapter
between the pipeline output and the external media layer.

When launching the pipeline, ForML :ref:`runner <runner>` expands the Sink into a closing
task within the assembled :ref:`workflow <workflow>` making it a native part of the final
DAG to be executed.

The core Sink API looks as follows:

.. autodata:: forml.io.Consumer

.. autoclass:: forml.io.Sink
    :members: consumer

.. autoclass:: forml.io.Sink.Writer
   :members: format, write


Sink Providers
--------------

The available Sink implementations are:

.. autosummary::
   :template: provider.rst
   :nosignatures:

   forml.provider.sink.stdout.Sink
