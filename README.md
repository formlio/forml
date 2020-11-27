<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

ForML
=====

[![GitHub Build](https://github.com/formlio/forml/workflows/CI%20Build/badge.svg)](https://github.com/formlio/forml/actions/)
[![Coverage Status](https://img.shields.io/codecov/c/github/formlio/forml/master.svg)](https://codecov.io/github/formlio/forml?branch=master)
[![Documentation Status](https://readthedocs.org/projects/forml/badge/?version=latest)](https://docs.forml.io/en/latest/)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![PyPI Version](https://badge.fury.io/py/forml.svg)](https://pypi.org/project/forml/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/forml)](https://pypi.org/project/forml/)
[![Code Style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


ForML is a framework for researching, implementing and operating data science projects.

Use ForML to formally describe a data science problem as a composition of high-level operators. ForML expands your
project into a task dependency graph specific to a given life-cycle phase and executes it using any of its supported
runners.

Solutions built on ForML are naturally easy to reuse, extend, reproduce, or share and collaborate on.


Not Just Another DAG
--------------------

Despite *DAG* (directed acyclic graph) being at the heart of ForML operations, it stands out amongst the many other task
dependency processing systems due to:

1. Its specialization on machine learning problems, that is wired right into the flow topology.
2. Concept of high-level operator composition which helps to wrap complex ML techniques into simple reusable units.
3. An abstraction of runtime dependencies allowing to run the same project using different technologies.


History
-------

ForML started as an open-source project in response to ever painful transitions of datascience research into production.
While there are other projects trying to solve this problem, they are typically either generic data processing systems
too low-level to provide out-of-the-box ML lifecycle routines or special scientific frameworks that are on the other
end too high-level to allow for robust operations.


Resources
---------

* [Documentation](https://docs.forml.io/en/latest/)
* [Source Code](https://github.com/formlio/forml/)
* Mailing lists:

  * Developers: `forml-dev@googlegroups.com`
  * Users: `forml-users@googlegroups.com`

* [Issue Tracker](https://github.com/formlio/forml/issues/)
* [PyPI Repository](https://pypi.org/project/forml/)
