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

.. _runner:

Pipeline Runner
===============

To perform particular :ref:`life cycle action <lifecycle>` of any given :ref:`project <project>`,
ForML delegates the :ref:`workflow topology <topology>` compiled into a :ref:`portable set
of instructions <topology-compiler>` to a selected :ref:`runner provider <runner-providers>` for
its execution.

The runner is the foremost elementary component of the :ref:`runtime platform <platform>` carrying
out the compute function on top of the entire IO layer (represented by the :ref:`feed <feed>`,
:ref:`sink <sink>` and the :ref:`registry <registry>` providers).

The pluggable provider model of the runner concept conveniently allows to mix and match different
processing technologies for different workloads as these typically come with varying
performance criteria regarding the particular use-case (e.g. low latency for online serving vs
large throughput for offline training).

There are three different :ref:`execution mechanisms <platform-execution>` each engaging the
pipeline runners under the hood.


Runner API
----------

.. autoclass:: forml.runtime.Runner
    :members: run


.. _runner-providers:

Runner Providers
----------------

Runner :ref:`providers <provider>` can be configured within the runtime :ref:`platform setup
<platform>` using the ``[RUNNER.*]`` sections.

The available implementations are:

.. autosummary::
   :template: provider.rst
   :nosignatures:

   forml.provider.runner.dask.Runner
   forml.provider.runner.graphviz.Runner
   forml.provider.runner.pyfunc.Runner
