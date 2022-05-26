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

Initial Setup
=============

Before diving into the actual implementation, we need to go through a couple of setup procedures.

Environment Configuration
-------------------------

Assuming you have no existing :doc:`feeds <../feed>` configured in your system yet, let's install the
:doc:`Openlake feed<openlake:install>`:

.. code-block:: console

    pip install --constraints https://raw.githubusercontent.com/formlio/openlake/main/constraints.txt 'openlake[kaggle]'


Let's now configure the local ForML :doc:`platform <../platform>` by adding the following content to your
:file:`~/.forml/config.toml`:

.. literalinclude:: config.toml
  :language: toml
  :start-after: # under the License.


Your local environment is now ready to perform all the runtime actions demonstrated in this tutorial.


Project Initialization
----------------------

Run the following shell command to create the initial :doc:`project structure<../project>`:

.. code-block:: console

    $ forml init --requirements=openschema,pandas,scikit-learn,numpy forml-tutorial-titanic

You should see a directory structure like this:

.. code-block:: console

    $ tree forml-tutorial-titanic
    forml-tutorial-titanic
    ├── application.py
    ├── setup.py
    └── titanic
        ├── __init__.py
        ├── evaluation.py
        ├── pipeline.py
        └── source.py


Source Definition
^^^^^^^^^^^^^^^^^

Let's edit the :file:`source.py` component supplying the project :ref:`datasource descriptor <project-source>` with
a :doc:`DSL query<../dsl>` against the particular :py:class:`Titanic schema from the Openschema catalog
<openschema:openschema.kaggle.Titanic>`:

.. literalinclude:: source.py
  :language: python
  :start-at: import


Evaluation Definition
^^^^^^^^^^^^^^^^^^^^^

Finally, we fill-in the :ref:`evaluation descriptor <project-evaluation>` within the :file:`evaluation.py` which
involves specifying the :doc:`evaluation strategy <../evaluation>` including the particular metric:

.. literalinclude:: evaluation.py
  :language: python
  :start-at: import
