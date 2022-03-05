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

Lifecycle
=========

Machine learning projects are operated in typical stages that are followed in a particular order. This pattern is what
we call a *lifecycle*. ForML supports two specific lifecycles depending on the project stage.

.. note::
   Do not confuse the lifecycles with *operational modes*. Forml projects can be operated in number of modes
   (:ref:`cli/batch <platform-cli>` - as used in the examples bellow, :doc:`interactively <interactive>` or using the
   :doc:`serving layer <serving>`) each of which is subject to a particular lifecycle.

.. _lifecycle-development:

Development Lifecycle
---------------------

This lifecycle is typically followed during the project development. All work is done in the scope of the project source
code working copy and no persistent modules are produced upon execution. It is typically managed using the
``python setup.py <mode>`` interface or in special case using the :doc:`interactive`. This lifecycle is supposed to aid
the development process allowing to quickly see the effect of the project changes.

The expected behaviour of the particular mode depends on the correct project setup as per the :doc:`project` sections.

The stages of a development lifecycle are:

Test
    Simply run through the unit tests defined as per the :doc:`testing` framework.

    Example::

        $ python3 setup.py test

Evaluate
    Perform a backtesting evaluation based on the specs defined in ``evaluation.py`` and report the metrics. The
    predicted outcomes are first derived using the user-configured method implementing techniques such as
    cross-validation or hold-out training. One of the potential use-cases might be a CI integration to continuously
    monitor (evaluate) the changes in the project development.

    Example::

        $ python3 setup.py eval


Tune
    Run hyper-parameter tuning reporting the results (not implemented yet).

    Example::

        $ python3 setup.py tune

Train
    Run the pipeline in the standard train mode. This will produce all the defined models but since it won't persist
    them, this mode is useful merely for testing the training (or displaying the task graph on the
    :doc:`Graphviz runner <runner/graphviz>`).

    Example::

        $ python3 setup.py train

Package
    Create the distributable project artifact containing all of its dependencies (produced into the ``dist`` directory
    under the project root directory).

    Example::

        $ python3 setup.py bdist_4ml

Upload
    Build and wrap the project into a runnable *Artifact* producing a new *Release* (that can then be used within
    the *Production Lifecycle*) and upload it to a persistent registry.

    .. note::
       Each particular registry allows uploading only distinct monotonically increasing releases per any given project,
       hence executing this stage twice against the same registry without incrementing the project version will fail.

    Example::

        $ python3 setup.py bdist_4ml upload


.. _lifecycle-production:

Production Lifecycle
--------------------

After publishing a project release in to a registry using the ``upload`` mode of the *research lifecycle*, the project
becomes available for the *production lifecycle*. Contrary to the research, this production lifecycle no longer needs
the project source code working copy as it operates solely on the published artifact plus potentially previously
persisted model generations.

The production lifecycle is either exercised in batch mode using :ref:`the CLI <platform-cli>` or
embedded within a :doc:`serving layer <serving>`. In any case, the stages of the production lifecycle are:

Train
    Fit (incrementally) the stateful parts of the pipeline using new labelled data producing a new *Generation* of
    the given release (unless explicit, the default release is the one with the highest version).

    Example::

        forml model train titanic

Tune
    Run hyper-parameter tuning of the selected pipeline and produce new *generation* (not implemented yet).

    Example::

        forml model tune titanic

Apply
    Run unlabelled data through a project *generation* (unless explicit, the default generation is the one with the
    highest version) producing transformed output (ie *predictions*).

    Example::

        forml model apply titanic

Evaluate
    Measure the actual performance of the model as predictions against the (previously unseen) true labelled data.

    Example::

        forml model eval titanic
