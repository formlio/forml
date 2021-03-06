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

To install the pre-packaged version of ForML simply use ``pip``::

    pip install forml

ForML has a number of optional features with their own dependencies which can be pulled in during the installation like
this::

    pip install 'forml[presto,dask]'

Some of the features might have their own binary dependencies that need to be resolved using a system-dependant way
(ie using the OS package manager).

Extra Features
--------------

+----------+---------------------------------------+----------------------------------------------------------------+
| Feature  | Install Command                       | Description                                                    |
+==========+=======================================+================================================================+
| all      | ``pip install 'forml[all]'``          | All extra features                                             |
+----------+---------------------------------------+----------------------------------------------------------------+
| dask     | ``pip install 'forml[dask]'``         | The Dask runner                                                |
+----------+---------------------------------------+----------------------------------------------------------------+
| dev      | ``pip install 'forml[dev]'``          | ForML development tools                                        |
+----------+---------------------------------------+----------------------------------------------------------------+
| doc      | ``pip install 'forml[doc]'``          | Documentation publishing dependencies                          |
+----------+---------------------------------------+----------------------------------------------------------------+
| flow     | ``pip install 'forml[flow]'``         | The standard operator and actor library shipped with ForML     |
+----------+---------------------------------------+----------------------------------------------------------------+
| graphviz | ``pip install 'forml[graphviz]'``     | The Graphviz pseudo-runner (also requires ``graphviz`` binary) |
+----------+---------------------------------------+----------------------------------------------------------------+
| presto   | ``pip install 'forml[presto]'``       | The PrestoDB reader                                            |
+----------+---------------------------------------+----------------------------------------------------------------+
