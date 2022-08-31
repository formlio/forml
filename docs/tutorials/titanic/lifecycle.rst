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

Lifecycle Actions
=================

After completing :doc:`the first version of our pipeline <pipeline>` component, the project is
ready to iterate through its :ref:`lifecycle <lifecycle-actions>`. Let's perform the standard
actions using the :ref:`CLI <platform-cli>` as the :ref:`execution mechanism <platform-execution>`.


Development Lifecycle
---------------------

1. Change directory to the root of the :file:`forml-tutorial-titanic` project working copy.
2. Let's first run all the :ref:`defined operator unit tests <titanic-pipeline-tests>` to confirm
   the project is in a good shape:

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

3. Try running the ``train`` action on the :class:`Graphviz runner
   <forml.provider.runner.graphviz.Runner>` (called ``visual`` in our :ref:`config
   <tutorial-setup>`) to see the train task graph:

   .. code-block:: console

       $ forml project train --runner visual

   .. image:: ../../_static/images/titanic-train.png
      :target: ../../_static/images/titanic-train.png

4. Run the ``eval`` action on the (default) :class:`Dask runner <forml.provider.runner.dask.Runner>`
   (called ``compute`` in our :ref:`config <tutorial-setup>`) to get the cross-validation score:

   .. code-block:: console

       $ forml project eval
       0.8379888268156425

   ...great, we've managed to improve from our :doc:`baseline workflow <exploration>`!

5. Create the project package artifact and upload it to the (default as per our :ref:`config
   <tutorial-setup>`) filesystem :ref:`registry <registry>` (assuming the same release doesn't
   already exist - otherwise increment the project version in the :ref:`setup.py <project-setup>`):

   .. code-block:: console

       $ forml project release

   This should publish the project into your local filesystem :ref:`model registry <registry>`
   making it available for the production lifecycle. It becomes the first published :ref:`release
   <registry-package>` of this project versioned as ``0.1.dev0`` (according to the version from
   :ref:`setup.py <project-setup>`).

Production Lifecycle
--------------------

:ref:`Production lifecycle <lifecycle-production>` doesn't need the project working copy, so feel
free to change the directory to another location before executing the commands.

1. List the local registry confirming the project has been published as its first release:

   .. code-block:: console

       $ forml model list
       forml-tutorial-titanic
       $ forml model list forml-tutorial-titanic
       0.1.dev0
       $ forml model list forml-tutorial-titanic 0.1.dev0

   The output shows the project artifact is available in the registry as a release ``0.1.dev0``
   not having any generation yet (the last command not producing any output).

3. Train the project (using the default runner as per our :ref:`config <tutorial-setup>`) to create
   the first :ref:`generation <registry-assets>` of its models and list the registry to confirm it
   got persisted:

   .. code-block:: console

       $ forml model train forml-tutorial-titanic
       $ forml model list forml-tutorial-titanic 0.1.dev0
       1

   Now we have our first :ref:`generation <registry-assets>` of the titanic models available in the
   registry.

3. Apply the trained generation to the testset to get the predictions:

   .. code-block:: console

       $ forml model apply forml-tutorial-titanic
       [0.38717846 0.37779938 0.38008973 0.37771585 0.3873835  0.38832168
       0.38671783 0.38736506 0.38115396 0.37622997 0.37642134 0.37965842
       ...
       0.3824376  0.38695502 0.38891135 0.38688363 0.38726499 0.37714804
       0.3860998  0.38041917 0.3885712 ]

4. Run the ``apply`` mode alternatively on the :class:`Graphviz
   <forml.provider.runner.graphviz.Runner>` runner to explore its task graph:

   .. code-block:: console

       $ forml model -R visual apply forml-tutorial-titanic

   .. image:: ../../_static/images/titanic-apply.png
      :target: ../../_static/images/titanic-apply.png

Now, after exploring two of the :ref:`execution mechanisms <platform-execution>` (namely the
:ref:`interactive <interactive>` mode demonstrated during the :doc:`exploratory
analysis <exploration>` and the :ref:`command-line driven <platform-cli>` batch processing shown
in this chapter), we can proceed to the final :doc:`deployment and serving <serving>`.
