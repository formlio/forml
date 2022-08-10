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

.. _evaluation:

Evaluation
==========

As part of the :ref:`project lifecycle management <lifecycle>`, ForML allows to *evaluate* the model
performance by quantifying the quality of its predictions using a number of different methods.

There are two different evaluation concepts each relating to one of the possible :ref:`lifecycles
<lifecycle-actions>`:

* :ref:`Development train-test evaluation <evaluation-traintest>` used to benchmark the solution
  during the development process
* :ref:`Production performance tracking <evaluation-perftrack>` used to monitor the
  health of the particular :ref:`model generation <lifecycle-generation>` while serving

All configuration specific to the evaluation setup is defined on the project level using the
:ref:`evaluation component <project-evaluation>` via the evaluation descriptor:

.. autoclass:: forml.project.Evaluation

With this information, ForML can assemble the particular evaluation :ref:`workflow <workflow>`
around the main :ref:`pipeline component <project-pipeline>` once that given lifecycle action
gets triggered.

.. attention::
    All the evaluation primitives described in this chapter deal with :ref:`flow topology
    <topology>` rather than any direct data values. Their purpose is not to do any calculation
    themselves but to construct the workflow that performs the evaluation when launched.
    For curiosity's sake, the :class:`Graphviz runner <forml.provider.runner.graphviz.Runner>` can
    be used to explore the particular DAGs composed in scope of an evaluation.

The evaluation process is in principle based on comparing *predicted* and *true* outcomes using
some :ref:`metric function <evaluation-metric>`. The evaluation API is using the
``evaluation.Outcome`` structure as pointers to the relevant DAG :ref:`ports <actor-ports>`
publishing these *predicted* and *true* outcomes:

.. autoclass:: forml.evaluation.Outcome
   :members: true, pred


.. _evaluation-metric:

Metric Function
---------------

The heart of the evaluation process is a specific *metric function* quantifying the quality of the
*predicted* versus *true* outcomes. There are dozens of standard metrics each suitable to different
scenarios (plus new ones can always be implemented ad-hoc).

The ForML evaluation API is using the following abstraction for its metric implementations:

.. autoclass:: forml.evaluation.Metric
   :members: score

Notable implementation of this ``Metric`` interface is the following ``Function`` class:

.. autoclass:: forml.evaluation.Function
   :show-inheritance:


.. _evaluation-traintest:

Development Train-Test Evaluation
---------------------------------

Continuous evaluation provides an essential feedback during the iterative :ref:`development process
<lifecycle-development>` indicating relative change in the solution quality induced by the
particular change in its implementation (code).

This type of evaluation is also referred to as *backtesting* since it involves *training* and
*testing* the solution on historical data with known outcomes. In another words, the *true* outcomes
are already know when producing the evaluation *prediction* outcomes.

There are different possible *methods* how the historical data can be correctly used within the
evaluated solution to essentially make predictions about the past. To generalize this concept
for the sake of the :ref:`workflow assembly <topology>`, ForML is using the following abstraction:

.. autoclass:: forml.evaluation.Method
   :members: produce

.. _evaluation-methods:

Methods
^^^^^^^

Following are the available :class:`evaluation.Method <forml.evaluation.Method>` implementations:

.. autoclass:: forml.evaluation.CrossVal
   :show-inheritance:

.. autoclass:: forml.evaluation.HoldOut
   :show-inheritance:


.. _evaluation-perftrack:

Production Performance Tracking
-------------------------------

After transitioning to the :ref:`production lifecycle <lifecycle-production>`, it becomes an
operational necessity to monitor the predictive performance of the deployed solution ensuring it
maintains its expected quality.

Natural tendency every model is exhibiting over time is its *drift* - gradual or sharp decline
between its learned generalization and the observed phenomena. This can have a number of different
reasons but the key measure is to detect it and to keep it under control by :ref:`refreshing
<lifecycle-generation>` or :ref:`reimplementing <lifecycle-release>` the model.

Continuous monitoring of the evaluation metric is the best way to spot these anomalies. This
process can also be referred to as the *serving evaluation* since its goal is to measure the
objective success while making the actual production decisions.

Process-wise, the performance tracking differs from the :ref:`development evaluation
<evaluation-traintest>` use-case in two key aspects:

#. It doesn't involve any *training* - the point is to evaluate predictions made by an existing
   :ref:`model generation <lifecycle-generation>` running in production. The concept of
   the different :ref:`methods <evaluation-methods>` known from the development evaluation doesn't
   apply - the :ref:`metric function <evaluation-metric>` scores directly the genuinely served
   predictions against the eventual true outcomes.
#. The *predictions* that are to be evaluated are in principle made before the *true* outcomes are
   known (real future predictions). This entails a dependency on an external reconciliation path
   a.k.a. *feedback loop* within the particular business application delivering the eventual true
   outcomes to which ForML simply plugs into using its :ref:`feed system <feed>`. The key attribute
   of this feedback loop is its *latency* which determines the turnaround time for the performance
   measurement (ranging from seconds to possibly months or more depending on the application).


ForML allows to report the serving evaluation metric based on the :ref:`project configuration
<project-evaluation>` by performing the relevant :ref:`lifecycle action <lifecycle-production>`.
