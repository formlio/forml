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

.. _interactive:

Interactive Mode
================

To enable practical *research and development* of ForML :ref:`projects <project>`, the framework
allows as one of its :ref:`execution mechanisms <platform-execution>` to compose and operate its
:ref:`workflows <workflow>` *interactively*. Generally, this can be utilized within REPL
(*read-evaluate-print-loop*) based terminals or more typically using high-level frontend interfaces
like the popular :doc:`Jupyter <jupyter:index>` notebooks.

.. note::
    ForML still remains grounded in the *code-first* principle of implementing the ML solution
    as a software :ref:`project <project>` as opposed to some of the native *notebook-first*
    oriented methodologies. The interactive mode is designed primarily for exploration rather than
    implementation of the eventual solution.

This chapter describes the individual tools allowing to use ForML interactively. Please refer to
the :ref:`tutorials <tutorials>` for actual examples demonstrating this principle in action.

Project Handle
--------------

To operate ForML projects interactively, the framework provides the following
programmatic interface allowing to access the :class:`project.Artifact <forml.project.Artifact>`
handle using either the :func:`project.open() <forml.project.open>` function or interactively
binding any :class:`project.Source <forml.project.Source>` instance with a custom pipeline using the
:meth:`.bind() <forml.project.Source.bind>` method.


.. autofunction:: forml.project.open

.. autoclass:: forml.project.Artifact
   :members: components, launcher


Virtual Launcher
----------------

The Virtual launcher represents one of the possible :ref:`execution mechanisms
<platform-execution>`. It is a wrapper around the low-level :ref:`runner <runner>` and :ref:`feed
<feed>` concepts designed specifically for *interactive* operations (internally it is also used by
the :ref:`testing framework <testing>`).


.. autoclass:: forml.runtime.Virtual
    :members: Trained
