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

Pipeline Runner
===============

The main point of the :doc:`runtime platform <../platform>` is to run the :doc:`projects <../project>`. The specific
implementation of a system that can :ref:`execute <concept-execution>` the project :doc:`lifecycle <../lifecycle>`
based on its description (its *Task Dependency Graph* in particular) is called the *Runner*.


.. _runner-virtual:

Virtual Launcher
----------------

Returned from :meth:`project.Artifact.launcher <forml.project.Artifact.launcher>`.

.. autoclass:: forml.runtime.Virtual


API
---

.. autoclass:: forml.runtime.Runner
    :members:


Providers
---------

The available runner implementations are:

.. autosummary::

   forml.provider.runner.dask
   forml.provider.runner.graphviz
   forml.provider.runner.pyfunc
