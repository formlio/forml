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

Common Setup
============

The tutorials depend on the following initial environment configuration:

Assuming you have no existing :doc:`feeds <../feed>` configured in your system yet, let's install the
:doc:`Openlake feed<openlake:install>`:

.. code-block:: console

    pip install --constraints https://raw.githubusercontent.com/formlio/openlake/main/constraints.txt 'openlake[kaggle]'


Let's now configure the local ForML :doc:`platform <../platform>` by adding the following content to your
:file:`~/.forml/config.toml`:

.. literalinclude:: ../../tutorials/config.toml
  :language: toml
  :start-after: # under the License.


Your local environment is now ready to perform all the runtime actions demonstrated in these tutorials.
