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

Lifecycle Management
====================

Machine learning projects are handled using a typical set of actions applied in a specific
order. This pattern is what we call a *lifecycle*. ForML supports two distinct lifecycles
depending on the project stage.

.. caution::
   Do not confuse the lifecycles with *processing mechanism*. Forml projects can be operated in a
   number of different ways (:ref:`cli/batch <platform-cli>` - as used in the examples bellow,
   :doc:`interactively <interactive>` or using the :doc:`serving engine <serving>`) each of which is
   subject to a particular lifecycle.


Iteration Accomplishment
------------------------

The ultimate milestone of each of the lifecycles is the point of producing (a new instance of)
the particular :ref:`runtime artifacts <registry-artifacts>`. This concludes the given iteration
and the process can start over and/or transit between the two lifecycles back and forth.

.. _lifecycle-generation:

Generation Advancement
^^^^^^^^^^^^^^^^^^^^^^

Whenever the given pipeline is *trained* (incrementally or from scratch) and/or *tuned*, a new
*generation* of the models is produced. ForML is implicitly tagging generations simply by
incremental *integer identifier* starting from ``1`` (relatively to the same *release*).

New generations are typically produced to refresh the models using new data
while keeping the same pipeline implementation. Updating the models of the same release allows (if
supported by the given models) to carry the state over from previous generations to the next by
incrementally training only on the new data obtained since the previous training.

Generations get transparently persisted in model registry as the :ref:`model generation assets
<registry-assets>`.

.. _lifecycle-release:

Release Roll-out
^^^^^^^^^^^^^^^

The milestone of the :ref:`development lifecycle <lifecycle-development>` is the roll-out of a new
*release*. It is essentially a new version of the project *code implementation* published for
deployment. Releases are identified using the explicit versioning specified by the user as per the
:ref:`project setup <project-setup>`.

Upon :ref:`packaging <lifecycle-package>` and :ref:`uploading <lifecycle-upload>`, releases get
persisted in the :ref:`model registry <registry-package>`.

.. caution::
   Given the different implementations, it is not possible to carry over states between generations
   of different releases.


Lifecycle Actions
-----------------

A simplified logical flow of the individual steps and interactions between the two lifecycles is
illustrated by the following diagram:


.. md-mermaid::
    graph TD
        subgraph development [Development Lifecycle]
        implement(Explore / Implement) --> traineval(Test + Evaluate)
        traineval -- Metrics --> release{Release?}
        release -- No --> implement
        release -- Yes --> package(Package) --> upload[(Upload)]
        end
        init((Init)) --> implement

        subgraph production [Production Lifecycle]
        renew{Renew?} -- Yes --> how{How?}
        how -- Refresh --> train
        how -- Reimplement --> implement
        upload -- Release Roll-out --> train[Train / Tune]
        train -- Generation Advancement --> apply([Apply / Serve])
        apply --> applyeval(Evaluate)
        applyeval -- Metrics --> renew
        renew -- No --> apply
        end

.. _lifecycle-development:

Development Lifecycle
^^^^^^^^^^^^^^^^^^^^^

As the name suggests, this lifecycle is exercised during the project development in scope of the
:doc:`project source-code <project>` working copy. It is typically managed using the ``python
setup.py <action>`` :ref:`CLI interface <platform-cli>` as shown bellow or using the :ref:`virtual
launcher <runner-virtual>` API when visited in the :doc:`interactive mode <interactive>`.

The expected behaviour of the particular action depends on the correct :doc:`project setup
<project>`.

.. hint::
   Any :ref:`model generations <lifecycle-generation>` produced within the development lifecycle
   are stored using a volatile registry which is not persistent across multiple python sessions.

The development lifecycle actions are:

Test
""""

Simply run through the unit tests defined as per the :doc:`testing` framework.

Example:

.. code-block:: console

    $ python3 setup.py test

Evaluate
""""""""

Perform the :ref:`train-test evaluation <evaluation-traintest>` based on the
:ref:`evaluation.py component <project-evaluation>` and report the metrics.

Example:

.. code-block:: console

    $ python3 setup.py eval

Train
"""""

Run the :ref:`project pipeline <project-pipeline>` in the standard :ref:`train mode
<workflow-mode>`. Even though this will produce a true generation of the defined models, it won't
get persisted across the invocations making this mode useful merely for smoke-testing the
training process (or displaying the task graph on the :doc:`Graphviz runner <runner/graphviz>`).

Example:

.. code-block:: console

    $ python3 setup.py train

.. _lifecycle-package:

Package
"""""""

Create the distributable project artifact containing all of its dependencies in form of a
:ref:`release package <registry-package>` (produced into the ``dist`` directory under the
project root directory).

Example:

.. code-block:: console

    $ python3 setup.py bdist_4ml

.. _lifecycle-upload:

Upload
""""""

Publish the *release package* into the configured :doc:`model registry<registry>`. This
effectively constitutes a :ref:`release <lifecycle-release>` and the process can transition
from here into the :ref:`production lifecycle <lifecycle-production>`.

.. warning::
   Each :doc:`model registry <registry>` allows uploading only unique monotonically
   increasing releases per any given project, hence executing this stage twice against the
   same registry without incrementing the :ref:`project version <project-setup>` is an error.

Example:

.. code-block:: console

    $ python3 setup.py bdist_4ml upload


.. _lifecycle-production:

Production Lifecycle
^^^^^^^^^^^^^^^^^^^^

After :ref:`rolling-out <lifecycle-release>` the new :ref:`release package <registry-package>`
into a registry, it becomes available for the *production lifecycle*. In contrast to the
development, the production lifecycle no longer needs the project source-code working copy as it
operates solely on that published release package (plus potentially the previously persisted
:ref:`model generations <registry-assets>`).

The production lifecycle is either managed in batch mode using the :ref:`CLI <platform-cli>` or
embedded within a :doc:`serving engine <serving>`.

The stages of the production lifecycle are:

Train
"""""

Run the :ref:`project pipeline <project-pipeline>` in the :ref:`train mode <workflow-mode>` to
produce :ref:`new generation <lifecycle-generation>` and persist it in the :doc:`model registry
<registry>`.

Example:

.. code-block:: console

    forml model train forml-example-titanic

Tune
""""

Run hyper-parameter tuning of the selected pipeline and produce new *generation* (not implemented
yet).

Example:

.. code-block:: console

    forml model tune forml-example-titanic

Apply
"""""

Run the previously trained :ref:`project pipeline <project-pipeline>` in the :ref:`apply
mode <workflow-mode>` using an existing :ref:`model generation <lifecycle-generation>` (explicit
version or by default the latest) loaded from the :doc:`model registry <registry>`.

Example:

.. code-block:: console

    forml model apply forml-example-titanic

.. seealso::
   In addition to this commandline based batch mechanism, the :doc:`serving engine <serving>`
   together with the :doc:`application concept <application>` is another way of performing the
   apply action of the production lifecycle.

Evaluate
""""""""

Perform the :ref:`production performance evaluation <evaluation-prod>` based on the
:ref:`evaluation.py component <project-evaluation>` and report the metrics.

Example:

.. code-block:: console

    forml model eval forml-example-titanic
