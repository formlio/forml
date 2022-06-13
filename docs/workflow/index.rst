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

Workflow Fundamentals
=====================

Workflow is the backbone of the ML solution responsible for consistently sticking all its pieces together. On the low
level it is a *Task Dependency Graph* with edges representing data flows and vertices standing for the data
transformations. This particular type of the graph is called *Directed Acyclic Graph* (DAG) - meaning the flows are
oriented and can't form any cycles. Representing workflows using task graphs is crucial for scalable scheduling,
distributed execution and runtime portability.

.. caution::
    At its core, the workflow internals explained in the following chapters are built around the Graph theory and SW+ML
    engineering principles, which might feel way too involved from a general data-science perspective. Fortunately, this
    level of detail is not required for the usual day-to-day work with the existing high-level ForML
    :doc:`operators <operator>`.

ForML is providing a convenient API for defining complex workflows using simple notation based on the following
concepts:

:doc:`Operators <operator>`
    are high-level pipeline macro-instructions that can be composed together and eventually expand into the task graph.

:doc:`Actors <actor>`
    are the low-level task primitives representing the graph vertices.

:doc:`Topology <topology>`
    is the particular interconnection of the individual actors determining their dependencies.

.. important::
    While the other ML frameworks and platforms out there are typically *model-centric* (having their discrete *train*
    process produce *model(s)* that get separately deployed for serving the *predict* phase), ForML, in contrast, is
    rather *workflow-centric* - ensuring all the steps (i.e. workflow) applied during the *predict* phase consistently
    reflect the original *train* process. That's achieved by an inseparable integration of both
    the *train* as well as the *apply* (predict) representations of the specific ML scenario into a single ForML
    expression. Essentially every single ForML workflow expands into one of the two related task graphs depending on
    its particular mode.

The high-level API for describing a workflow allows to compose an operator expressions using the following syntax:

.. code-block:: python

    flow = LabelExtractor(column='foo') >> NaNImputer() >> RFC(max_depth=3)

The typically counterintuitive feature of any DAG-based frameworks is that execution of these expressions *builds* a DAG
rather than *performing* the actual processing functions (which happens separately in a completely different context).

Given the implementation of the particular operators used in the previous example, this single expression might render
a workflow with the two *train* and *apply* task graphs visualized as follows:

.. md-mermaid::
    :name: flowcharts

    graph TD
        TNT["NaNImputer().train"] -. state .-> ANA["NaNImputer().setstate.apply"]
        TRFC["RFC(max_depth=3).train"] -. state .-> ARFC["RFC(max_depth=3).setstate.apply"]
        subgraph Train Mode
        TF((Future)) --> TLE["LabelExtractor(column='foo').apply"];
        TLE --> TG0(["Getter#0"]);
        TLE --> TG1(["Getter#1"]);
        TG0 -- features --> TNT;
        TG1 -- labels --> TNT;
        TG0 -- features --> TNA["NaNImputer().setstate.apply"];
        TNT -- state --> TNA;
        TNA --> TRFC;
        TG1 -- labels --> TRFC;
        end
        subgraph Apply Mode
        AF((Future)) --> ANA
        ANA --> ARFC
        end

The meaning of :doc:`operators <operator>` and how they are defined using :doc:`actors <actor>` and their
:doc:`interconnections <topology>` is described in details in the following chapters:

.. toctree::
    :maxdepth: 2

    actor
    topology
    operator
