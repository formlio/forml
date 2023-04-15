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

ForML Documentation
===================

ForML is a development framework for researching and implementing data science projects as well
as an MLOps platform capable of managing their entire life cycles.

Use ForML to formally describe a data science problem as a composition of high-level operators.
ForML expands your project into a task dependency graph specific to the given life-cycle phase and
executes it using any of its supported technologies while taking care of all of its operational
requirements.

Solutions built on ForML are naturally easy to reuse, extend, reproduce, or share and
collaborate on.


Not Just Another DAG
--------------------

Despite DAG (directed acyclic graph) being at the heart of ForML operations, it stands out among
the many other task dependency processing systems due to its:

#. Specialization in machine learning problems wired right into the flow topology.
#. Concept of high-level operator composition helping to wrap complex ML techniques into simple
   reusable units.
#. Abstraction of runtime dependencies allowing to implement fully portable projects that can be
   operated interchangeably using different technologies.


History
-------

ForML started as a response addressing the notoriously painful process of transitioning any
data science research into production. The framework was initially developed by a group of
data scientists and ML engineers seeking to minimize the effort traditionally required to
productionize any typical ML solution. Becoming increasingly useful to its original authors,
ForML has been soon released as a community-driven project.


Resources
---------

* `Documentation <https://docs.forml.io/en/latest/>`_
* `Source Code <https://github.com/formlio/forml/>`_
* `Chat Room <https://app.gitter.im/#/room/#formlio_community:gitter.im>`_
* `Issue Tracker <https://github.com/formlio/forml/issues/>`_
* `PyPI Repository <https://pypi.org/project/forml/>`_


.. toctree::
    :hidden:

    Home <self>
    license


.. toctree::
    :hidden:
    :caption: Getting Started

    install
    principles
    tutorials/index


.. toctree::
    :hidden:
    :caption: Framework Manual

    project
    workflow/index
    evaluation
    testing
    application
    pipeline


.. toctree::
    :hidden:
    :caption: Data Management

    io
    dsl/index
    feed
    sink
    registry
    inventory


.. toctree::
    :hidden:
    :caption: Runtime Operations

    lifecycle
    platform
    runner
    interactive
    serving


.. toctree::
    :hidden:

    provider
