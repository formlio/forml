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

ForML is a framework for researching, implementing and operating data science projects.

Use ForML to formally describe a data science problem as a composition of high-level operators.
ForML expands your project into a task dependency graph specific to given life-cycle phase and executes it using any of
its supported runners.

Solutions built on ForML are naturally easy to reuse, extend, reproduce, or share and collaborate on.


Not Just Another DAG
--------------------

Despite *DAG* (directed acyclic graph) being at the heart of ForML operations, it stands out amongst the many other task
dependency processing systems due to:

1. Its specialization on machine learning problems that's wired right into the flow topology.
2. Concept of high-level operator composition which helps wrapping complex ML techniques into simple reusable units.
3. Abstraction of runtime dependencies allowing to run the same project using different technologies.


History
-------

ForML started as an open-source project in response to ever painful transitions of datascience research into production.
While there are other projects trying to solve this problem, they are typically either generic data processing systems
too low-level to provide out-of-the-box ML lifecycle routines or special scientific frameworks that are on the other
end too high-level to allow for robust operations.


Resources
---------

* `Documentation <https://docs.forml.io/en/latest/>`_
* `Source Code <https://github.com/formlio/forml/>`_
* Mailing lists:

  * Developers: forml-dev@googlegroups.com
  * Users: forml-users@googlegroups.com

* `Issue Tracker <https://github.com/formlio/forml/issues/>`_
* `PyPI Repository <https://pypi.org/project/forml/>`_


Content
-------

.. toctree::
    :maxdepth: 2
    :caption: Getting Started

    concept
    license
    install
    examples
    tutorial
    faq

.. toctree::
    :maxdepth: 2
    :caption: Implementor's Guide

    project
    lifecycle
    workflow
    io
    dsl/index
    interactive
    operator
    testing
    lib

.. toctree::
    :maxdepth: 2
    :caption: Runtime Manual

    platform
    feed
    registry/index
    runner/index
    sink
