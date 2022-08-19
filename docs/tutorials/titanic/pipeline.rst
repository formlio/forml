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

Pipeline Implementation
=======================

After finishing the :doc:`exploration <exploration>`, we can proceed to implementing the actual
ML solution in form of the ForML project :ref:`pipeline component <project-pipeline>`.

For cleaner structuring, we change the ``pipeline`` component from a flat module to a hierarchical
package (which is :ref:`semantically identical <project-principal>`) and add a skeleton for our
``titanic.pipeline.preprocessing`` module together with its unit tests under
:file:`tests/pipeline/test_preprocessing.py` that we are going to implement later.
The project structure now looks as follows:

.. code-block:: console

    $ tree forml-tutorial-titanic
    forml-tutorial-titanic
    ├── notebooks
    │         └── exploration.ipynb
    ├── tests
    │         ├── pipeline
    │         │         ├── __init__.py
    │         │         └── test_preprocessing.py
    │         └── __init__.py
    ├── titanic
    │         ├── pipeline
    │         │         ├── __init__.py
    │         │         └── preprocessing.py
    │         ├── __init__.py
    │         ├── evaluation.py
    │         └── source.py
    └── setup.py


Custom Preprocessing Operators
------------------------------

In addition to the ``Imputer`` operator we've created in scope of our :doc:`exploration
<exploration>`, let's improve our preprocessing with a couple more operators. We stick to the
simple ``@wrap`` technique for implementing :ref:`actors <actor-decorated>` and eventually
:ref:`operators <operator-wrapped>`.

ParseTitle
^^^^^^^^^^

Let's start with a simple stateless transformer extracting the *title* from the *name* creating a
new column (``target``) and dropping the original (``source``) with the name:

.. literalinclude:: ../../../tutorials/titanic/titanic/pipeline/preprocessing.py
  :caption: titanic/pipeline/preprocessing.py
  :linenos:
  :language: python
  :pyobject: ParseTitle

Encode
^^^^^^

The :class:`OneHotEncoder <sklearn:sklearn.preprocessing.OneHotEncoder>` we used in our
:doc:`baseline workflow <exploration>` was applied bluntly to all columns including those
non-categorical ones. Let's improve it by creating a custom operator with parametrized selection of
the encoded columns:

.. literalinclude:: ../../../tutorials/titanic/titanic/pipeline/preprocessing.py
  :caption: titanic/pipeline/preprocessing.py
  :linenos:
  :language: python
  :start-after: sphinx: Encode start
  :end-before: sphinx: Encode end

.. _titanic-pipeline-tests:

Writing Unit Tests
------------------

As a best practice, let's define :ref:`unit tests <testing>` for our operators.

.. literalinclude:: ../../../tutorials/titanic/tests/pipeline/test_preprocessing.py
  :caption: tests/pipeline/test_preprocessing.py
  :linenos:
  :language: python
  :start-at: import


Pipeline Expression
-------------------

With all of the preprocessing operators now ready, we can proceed to defining the actual
:ref:`workflow expression <workflow-expression>` for the :ref:`pipeline component
<project-pipeline>` within the :file:`titanic/pipeline/__init__.py`.

Unlike in our :doc:`baseline workflow <exploration>`, we are going to use multiple classifiers
stacked together using the :class:`pipeline.ensemble.FullStack <forml.pipeline.ensemble.FullStack>`
ensembler. Each of the individual models is native :doc:`Scikit-learn classifier
<sklearn:supervised_learning>` auto-wrapped into a ForML operator using the
:func:`pipeline.wrap.importer() <forml.pipeline.wrap.importer>` context manager.

The file again ends with
call to the :func:`project.setup() <forml.project.setup>` to register the component within the
framework.

.. literalinclude:: ../../../tutorials/titanic/titanic/pipeline/__init__.py
  :caption: titanic/pipeline/__init__.py
  :linenos:
  :language: python
  :start-at: import


:ref:`Component-wise <project-structure>`, this makes our project complete, allowing us to
further :doc:`progress its lifecycle <lifecycle>`.
