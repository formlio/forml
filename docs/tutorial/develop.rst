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

Codebase Implementation
=======================

Project Operations
------------------

We will exercise the standard :doc:`lifecycle <lifecycle>` actions.

Development Lifecycle Actions
'''''''''''''''''''''''''''''

1. Change directory to the root of the ``titanic`` project working copy.
2. Let's first run all the operator unit tests to confirm the project is in good shape::

    $ python3 setup.py test
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

3. Try running the ``train`` mode on the *Graphviz* runner (called ``visual`` in our config ) to see the train task
   graph::

    $ python3 setup.py train --runner visual

.. image:: _static/images/titanic-train.png
   :align: center

4. Run the ``eval`` mode on the (default) *Dask* runner (called ``compute`` in our config) to get the
   cross-validation score::

    $ python3 setup.py eval
    0.8379888268156425

5. Create the project package artifact and upload it to the (default) filesystem registry (assuming the same release
   doesn't already exist - otherwise increment the project version in the ``setup.py``)::

    $ python3 setup.py bdist_4ml upload

   This should publish the project into your local filesystem forml registry making it available for the production
   lifecycle. This becomes the first published :ref:`release <concept-persistence>` of this project versioned (according
   to the version from :ref:`setup.py <project-setup>` as ``0.1.dev0``)

Production Lifecycle Actions
''''''''''''''''''''''''''''

Production lifecycle doesn't need the project working copy so feel free to change the directory to another location
before executing the commands.

1. List the local registry confirming the project has been published as its first release::

    $ forml model list
    forml-example-titanic
    $ forml model list forml-example-titanic
    0.1.dev0
    $ forml model list forml-example-titanic 0.1.dev0

   The output shows the project artifact is available in the registry as a release ``0.1.dev0`` not having any
   generation yet (the last command not producing any output).

2. Train the project (using the default runner as per our config) to create the first generation of its models and list
the registry to confirm it got persisted::

    $ forml model train forml-example-titanic
    $ forml model list forml-example-titanic 0.1.dev0
    1

   Now we have our first generation of the titanic models available in the registry.

3. Apply the trained generation to get the predictions::

    $ forml model apply forml-example-titanic
    [0.38717846 0.37779938 0.38008973 0.37771585 0.3873835  0.38832168
    0.38671783 0.38736506 0.38115396 0.37622997 0.37642134 0.37965842
    ...
    0.3824376  0.38695502 0.38891135 0.38688363 0.38726499 0.37714804
    0.3860998  0.38041917 0.3885712 ]


4. Run the ``apply`` mode alternatively on the *Graphviz* runner to explore its task graph::

    $ forml model -R visual apply forml-example-titanic

.. image:: _static/images/titanic-apply.png

Serving
'''''''

Working with Jupyter Notebooks
------------------------------

See the tutorial notebook stored in the demo project under ``examples/tutorial/titanic/notebooks/tutorial.ipynb`` for
a step-by-step examples of working with ForML project in Jupyter.

Further details on the interactive style of work with ForML in general can be found in the :doc:`interactive` sections.
