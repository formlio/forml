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

Titanic Challenge
=================

There is a complete ForML project available under ``tutorials/titanic/``. It is the famous
`Titanic Challenge <https://www.kaggle.com/c/titanic>`_. We will use it here to demonstrate the
typical ForML usecases.


Initial Setup
-------------

Before diving into the actual implementation, we need to go through a couple of setup procedures.


Initialization
^^^^^^^^^^^^^^

Run the following shell command to create the initial :doc:`project structure<../../project>`:

.. code-block:: console

    $ forml project init --requirements=openschema,pandas,scikit-learn,numpy forml-tutorial-titanic

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

Let's edit the :file:`source.py` component supplying the project :ref:`datasource descriptor
<project-source>` with a :doc:`DSL query<../../dsl>` against the particular :py:class:`Titanic
schema from the Openschema catalog <openschema:openschema.kaggle.Titanic>`:

.. literalinclude:: ../../tutorials/titanic/titanic/source.py
  :language: python
  :start-at: import


Evaluation Definition
^^^^^^^^^^^^^^^^^^^^^

Finally, we fill-in the :ref:`evaluation descriptor <project-evaluation>` within the
:file:`evaluation.py` which involves specifying the :doc:`evaluation strategy <../evaluation>`
including the particular metric:

.. literalinclude:: ../../tutorials/titanic/titanic/evaluation.py
  :language: python
  :start-at: import


Exploration with Jupyter
------------------------

See the tutorial notebook stored in the demo project under
``examples/tutorial/titanic/notebooks/tutorial.ipynb`` for a step-by-step examples of working
with ForML project in Jupyter.

Further details on the interactive style of work with ForML in general can be found in the
:doc:`../interactive` sections.


Lifecycle Actions
-----------------

We will exercise the standard :ref:`lifecycle actions <lifecycle-actions>`.

Development Lifecycle Actions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Change directory to the root of the ``titanic`` project working copy.
2. Let's first run all the operator unit tests to confirm the project is in good shape:

   .. code-block:: console

       $ forml project test
       running test
       TestNaNImputer
       Test of Invalid Params ... ok
       TestNaNImputer
       Test of Not Trained ... ok
       TestNaNImputer
       Test of Valid Imputation ... ok
       TestTitleParser
       Test of Invalid Params ... ok
       TestTitleParser
       Test of Invalid Source ... ok
       TestTitleParser
       Test of Valid Parsing ... ok
       ----------------------------------------------------------------------
       Ran 6 tests in 0.591s

       OK

3. Try running the ``train`` action on the *Graphviz* runner (called ``visual`` in our config ) to
   see the train task graph:

   .. code-block:: console

       $ forml project train --runner visual

   .. image:: ../_static/images/titanic-train.png
      :align: center

4. Run the ``eval`` action on the (default) *Dask* runner (called ``compute`` in our config) to get
   the cross-validation score:

   .. code-block:: console

       $ forml project eval
       0.8379888268156425

5. Create the project package artifact and upload it to the (default) filesystem registry
   (assuming the same release doesn't already exist - otherwise increment the project version in
   the :file:`setup.py`):

   .. code-block:: console

       $ forml project release

   This should publish the project into your local filesystem :doc:`model registry <../registry>`
   making it available for the production lifecycle. This becomes the first published :ref:`release
   <concept-persistence>` of this project versioned (according to the version from :ref:`setup.py
   <project-setup>` as ``0.1.dev0``)

Production Lifecycle Actions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:ref:`Production lifecycle <lifecycle-production>` doesn't need the project working copy so feel
free to change the directory to another location before executing the commands.

1. List the local registry confirming the project has been published as its first release:

   .. code-block:: console

       $ forml model list
       forml-example-titanic
       $ forml model list forml-example-titanic
       0.1.dev0
       $ forml model list forml-example-titanic 0.1.dev0

   The output shows the project artifact is available in the registry as a release ``0.1.dev0``
   not having any generation yet (the last command not producing any output).

3. Train the project (using the default runner as per our config) to create the first generation
   of its models and list the registry to confirm it got persisted:

   .. code-block:: console

       $ forml model train forml-example-titanic
       $ forml model list forml-example-titanic 0.1.dev0
       1

   Now we have our first generation of the titanic models available in the registry.

3. Apply the trained generation to get the predictions:

   .. code-block:: console

       $ forml model apply forml-example-titanic
       [0.38717846 0.37779938 0.38008973 0.37771585 0.3873835  0.38832168
       0.38671783 0.38736506 0.38115396 0.37622997 0.37642134 0.37965842
       ...
       0.3824376  0.38695502 0.38891135 0.38688363 0.38726499 0.37714804
       0.3860998  0.38041917 0.3885712 ]

4. Run the ``apply`` mode alternatively on the *Graphviz* runner to explore its task graph:

   .. code-block:: console

       $ forml model -R visual apply forml-example-titanic

   .. image:: ../_static/images/titanic-apply.png


Deployment and Serving
----------------------

TODO

Serving
^^^^^^^
