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

.. _pipeline:

Pipeline Library
================

ForML comes with a number of useful :ref:`operators <operator>`, :ref:`actors <actor>`, and general
utility functions ready to be engaged in *pipeline implementations*.

The library is not essential to any of the ForML base functionality and rather offers additional
extensions on top of the core API. It is currently not particularly rich but some of the included
high-level entities can already boost the typical pipeline development process or possibly serve
as a point of reference demonstrating the power of the underlying ForML :ref:`workflow
architecture <workflow>`.

This pipeline library is organized into the following functionally related modules:

.. autosummary::
   :template: pipeline.rst
   :toctree: _auto

    forml.pipeline.wrap
    forml.pipeline.payload
    forml.pipeline.ensemble
