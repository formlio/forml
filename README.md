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

[![GitHub Build](https://img.shields.io/github/workflow/status/formlio/forml/CI%20Build/main)](https://github.com/formlio/forml/actions/)
[![Coverage Status](https://img.shields.io/codecov/c/github/formlio/forml/main)](https://codecov.io/github/formlio/forml?branch=main)
[![Code Style: black](https://img.shields.io/badge/code%20style-black-000000)](https://github.com/psf/black)
[![Lines of Code](https://img.shields.io/tokei/lines/github/formlio/forml)](https://github.com/formlio/forml)

[![Documentation Status](https://readthedocs.org/projects/forml/badge/?version=latest)](https://docs.forml.io/en/latest/)
[![License](https://img.shields.io/pypi/l/forml)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Gitter](https://badges.gitter.im/formlio/community.svg)](https://gitter.im/formlio/community/)

![Python Version](https://img.shields.io/pypi/pyversions/forml)
[![PyPI Version](https://img.shields.io/pypi/v/forml)](https://pypi.org/project/forml/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/forml)](https://pypi.org/project/forml/)

[![Stars](https://img.shields.io/github/stars/formlio/forml)](https://github.com/formlio/forml/stargazers)
[![Forks](https://img.shields.io/github/forks/formlio/forml)](https://github.com/formlio/forml/fork)
[![Issues](https://img.shields.io/github/issues/formlio/forml)](https://github.com/formlio/forml/issues)
[![Pull Requests](https://img.shields.io/github/issues-pr/formlio/forml)](https://github.com/formlio/forml/pulls)
[![Contributors](https://img.shields.io/github/contributors/formlio/forml)](https://github.com/formlio/forml/graphs/contributors)
[![Last Commit](https://img.shields.io/github/last-commit/formlio/forml)](https://github.com/formlio/forml/commits/main)

ForML is a development framework for researching and implementing data science projects as well
as an MLOps platform capable of managing their entire lifecycles.

Use ForML to formally describe a data science problem as a composition of high-level operators.
ForML expands your project into a task dependency graph specific to the given life-cycle phase and
executes it using any of its supported technologies while taking care of all of its operational
requirements.

Solutions built on ForML are naturally easy to reuse, extend, reproduce, or share and
collaborate on.


Not Just Another DAG
--------------------

Despite *DAG* (directed acyclic graph) being at the heart of ForML operations, it stands out
amongst the many other task dependency processing systems due to:

1. Its specialization on machine learning problems, that is wired right into the flow topology.
2. Concept of high-level operator composition which helps to wrap complex ML techniques into
   simple reusable units.
3. An abstraction of runtime dependencies allowing to implement fully portable projects that can
   be operated interchangeably using different technologies.


History
-------

ForML started as a response addressing the notoriously painful process of transitioning any
data science research into production. The framework was initially developed by a group of
data scientists and ML engineers seeking to minimize the effort traditionally required to
productionize any typical ML solution. Becoming increasingly useful to its original authors,
ForML has been soon released as a community driven project.


Resources
---------

* [Documentation](https://docs.forml.io/en/latest/)
* [Source Code](https://github.com/formlio/forml/)
* [Chat Room](https://gitter.im/formlio/community/)
* [Issue Tracker](https://github.com/formlio/forml/issues/)
* [PyPI Repository](https://pypi.org/project/forml/)
