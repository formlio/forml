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

Installation
============


Getting ForML
-------------

To install the pre-packaged version of ForML simply use ``pip``:

.. code-block:: console

    $ pip install forml


Dependency Constraints
----------------------

Being a framework, the ForML distribution package comes with unpinned dependencies to give enough
flexibility to the client applications. For reproducibility, there is a ``constraints.txt``
provided to offer a known-to-be-working combination of all of the dependencies used by ForML. To
install ForML using these dependencies, simply add the ``--constraint`` to (any of the mentioned)
``pip install`` commands:

.. code-block:: console

    $ pip install --constraint https://raw.githubusercontent.com/formlio/forml/main/constraints.txt forml

Extra Features
--------------

ForML has several optional features with their own dependencies which can be pulled in during the
installation like this:

.. code-block:: console

    $ pip install 'forml[sql,dask]'

Some of the features might have additional binary dependencies that need to be resolved using a
system-dependant mechanism (i.e. using the OS package manager).

+----------+---------------------------------------+----------------------------------------------------------------+
| Feature  | Install Command                       | Description                                                    |
+==========+=======================================+================================================================+
| all      | ``pip install 'forml[all]'``          | All extra features                                             |
+----------+---------------------------------------+----------------------------------------------------------------+
| dask     | ``pip install 'forml[dask]'``         | The Dask runner                                                |
+----------+---------------------------------------+----------------------------------------------------------------+
| dev      | ``pip install 'forml[dev]'``          | ForML development tools                                        |
+----------+---------------------------------------+----------------------------------------------------------------+
| docs     | ``pip install 'forml[docs]'``         | Documentation publishing dependencies                          |
+----------+---------------------------------------+----------------------------------------------------------------+
| graphviz | ``pip install 'forml[graphviz]'``     | The Graphviz pseudo-runner (also requires ``graphviz`` binary) |
+----------+---------------------------------------+----------------------------------------------------------------+
| mlflow   | ``pip install 'forml[mlflow]'``       | Model registry backed by the MLFlow Tracking server            |
+----------+---------------------------------------+----------------------------------------------------------------+
| rest     | ``pip install 'forml[rest]'``         | RESTful serving gateway                                        |
+----------+---------------------------------------+----------------------------------------------------------------+
| sql      | ``pip install 'forml[sql]'``          | SQL reader dependencies                                        |
+----------+---------------------------------------+----------------------------------------------------------------+
