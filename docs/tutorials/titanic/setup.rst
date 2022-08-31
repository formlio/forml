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

Before diving into the actual implementation, we need to go through a couple of setup procedures
(in addition to those :ref:`common to all tutorials <tutorial-setup>`).


Starting a New Project
----------------------

Run the following shell command to create the initial :ref:`project structure <project>`:

.. code-block:: console

    $ forml project init --requirements=openschema,pandas,scikit-learn,numpy --package titanic forml-tutorial-titanic

You should see a directory structure like this:

.. code-block:: console

    $ tree forml-tutorial-titanic
    forml-tutorial-titanic
    ├── titanic
    │   ├── __init__.py
    │   ├── evaluation.py
    │   ├── pipeline.py
    │   └── source.py
    └── setup.py


Source Definition
-----------------

Let's edit the :file:`source.py` component supplying the project :ref:`datasource descriptor
<project-source>` with a :ref:`DSL query <dsl>` against the particular :py:class:`Titanic
schema from the Openschema catalog <openschema:openschema.kaggle.Titanic>`. Note the essential call
to the :func:`project.setup() <forml.project.setup>` at the end registering the component within
the framework.

.. literalinclude:: ../../../tutorials/titanic/titanic/source.py
  :caption: titanic/source.py
  :linenos:
  :language: python
  :start-at: import


Evaluation Definition
---------------------

Finally, we fill-in the :ref:`evaluation descriptor <project-evaluation>` within the
:file:`evaluation.py` which involves specifying the :ref:`evaluation strategy <evaluation>`
including the particular metric. The file again ends with call to the :func:`project.setup()
<forml.project.setup>` to register the component within the framework.

.. literalinclude:: ../../../tutorials/titanic/titanic/evaluation.py
  :caption: titanic/evaluation.py
  :linenos:
  :language: python
  :start-at: import
