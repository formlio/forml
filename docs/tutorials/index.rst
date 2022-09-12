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

.. _tutorials:

Tutorials
=========

The easiest way to get started with ForML is to go through its practical tutorials presented in
this chapter. Assuming you have already installed ForML as per the :ref:`installation instructions
<install>` and ideally also familiarized yourself with the :ref:`ForML principles <principles>`,
you can now go straight through the following list of step-by-step examples demonstrating the
ForML capabilities.

.. _tutorial-setup:
.. rubric:: Common Setup

The tutorials depend on the following initial environment configuration:

Assuming you have no existing :ref:`feeds <feed>` configured in your system yet, let's install
the :doc:`Openlake feed <openlake:install>`:

.. code-block:: console

    $ pip install --constraints https://raw.githubusercontent.com/formlio/openlake/main/constraints.txt 'openlake[kaggle]'


Let's now configure the local ForML :ref:`platform <platform>` by adding the following content to
your :file:`~/.forml/config.toml`:

.. literalinclude:: ../../tutorials/config.toml
  :language: toml
  :start-after: # under the License.

.. important::
    Make sure to configure your Kaggle API access token under the :file:`~/.kaggle/kaggle.json`
    as described in the `Kaggle API Documentation <https://www.kaggle.com/docs/api>`_ to get
    access to all of the datasets used in this tutorial.

Your local environment is now ready to perform all the runtime actions demonstrated in these
tutorials.


.. rubric:: Tutorials List

The list of available tutorials is:

* The :ref:`Titanic challenge <titanic>` is a *complete* end-to-end ML :ref:`project <project>`
  implemented using ForML.
* :ref:`Pipeline demos <demos>` represent a set of small *snippets* demonstrating the :ref:`pipeline
  composition <workflow>` fundamentals.


.. toctree::
    :hidden:

    titanic/index
    demos
