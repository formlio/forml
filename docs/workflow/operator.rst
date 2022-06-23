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

Operator Architecture
=====================

Operators are the high-level workflow entities used for implementing the actual ML pipeline
expressions. They can be seen as dynamic macro-instructions expanding the particular task graph
based on their composition logic. This is a very powerful concept as it completely abstracts away
the internal wiring complexity of the low-level :doc:`task graph assembly <topology>` providing a
simple interface for the pipeline expressions.

If parametrized (rather than hard-coded) with the particular actor implementations, operators can
be independent of the actual data types and formats as they deal purely with the topology.
Therefore, many operators can be shared as library components turning advanced techniques into
reusable commodity-like modules (see the ensembler).

Built upon the *pipeline mode duality principle*, operators always deliver the related task graphs
for both of the *train* and *predict* modes together. That's how ForML enforces the train-predict
integrity at every step of the workflow.

The operator layer also happens to be the ideal stage for carrying out unit testing. For this
purpose, ForML provides a complete :doc:`operator unit testing framework <../testing>`.


Generic Implementation
----------------------

Operators can implement whatever complex functionality based on any number of actors. They are
using the :ref:`logical topology structures <topology-logical>` to define the internal task
graph and its composition with the preceding operators.

The base abstract class for implementing operators is the ``flow.Operator``:

.. autoclass:: forml.flow.Operator
   :members: compose, expand


Let's explain the operator development process by implementing a typical *Stateful Mapper*
operator. Conceptually, this operator works as follows:

#. in train-mode:

   #. it first gets *trained* (*Task 1* - ``.train()``) using the train features (via *Train*
      port) and labels (via *Label* port)
   #. then, using the state acquired during the training task, it *maps* (*Task 2* - ``.apply()``)
      the *train features* (via *Apply input* port) producing the transformed output (via
      *Apply output* port)

#. in apply-mode:

   #. again, using the state acquired during the training task, it *maps* (*Task 3* - ``.apply()``)
      this time the *apply features* (via *Apply input* port) producing the transformed output
      (via *Apply output* port)

The following diagram outlines the flows:


.. md-mermaid::

    graph LR
        subgraph Mapper Worker Group
        tt["mapper@train-mode.train()"]
        ta(["mapper@train-mode.apply()"])
        aa(["mapper@apply-mode.apply()"])
        tt -. state .-> ta & aa
        end
        subgraph Trunk Heads
        ti((T)) --> tt & ta
        li((L)) -- L --> tt
        ai((A)) --> aa
        end
        subgraph Trunk Tails
        ta --> to((T))
        li -- L --> lo((L))
        aa --> ao((A))
        end

The segment between the ``A`` head/tail nodes represents the apply-mode task graph, while the
segment between the ``T`` (+ ``L``) nodes represents the train-mode task graph.

Proceeding to the actual implementation, we simply extend the ``flow.Operator`` class and provide
the ``.compose()`` method:

.. code-block:: python
    :linenos:

    from forml import flow

    class StatefulMapper(flow.Operator):
        """Generic stateful mapper operator."""

        def __init__(self, actor_builder: flow.Builder):
            assert actor_builder.actor.is_stateful(), 'Stateful expected'
            self._actor_builder = actor_builder

        def compose(self, scope: flow.Composable) -> flow.Trunk:
            preceding: flow.Trunk = scope.expand()
            mapper_trainmode_train = flow.Worker(self._actor_builder, 1, 1)
            mapper_trainmode_apply = mapper_trainmode_train.fork()
            mapper_applymode_apply = mapper_trainmode_train.fork()
            mapper_trainmode_train.train(preceding.train.publisher, preceding.label.publisher)
            return preceding.extend(mapper_applymode_apply, mapper_trainmode_apply)


We can see the three workers (forked from the common instance to make them part of the same
:ref:`worker group <topology-state>`) attached to the relevant segments of the preceding trunk.
Note the operator is truly generic as the actual actor implementing the particular mapping
function is provided as a parameter.


.. _operator-composition:

Operator Composition
--------------------

Given the ``mean_impute`` example actor :ref:`implemented earlier <actor-decorated>`, we can now
create two imputation operators and use them to compose a simple workflow using the ``>>`` syntax:

.. code-block:: python

    impute_foo = StatefulMapper(MeanImpute.builder(column='foo'))
    impute_bar = StatefulMapper(MeanImpute.builder(column='bar'))

    pipeline = impute_foo >> impute_bar


That would render the following task graphs:

.. md-mermaid::

    graph TD
        subgraph Foo Worker Group
        tft["foo@train-mode.train()"]
        tfa(["foo@train-mode.apply()"])
        afa(["foo@apply-mode.apply()"])
        tft -. state .-> tfa & afa
        end
        subgraph Bar Worker Group
        tbt["bar@train-mode.train()"]
        tba(["bar@train-mode.apply()"])
        aba(["bar@apply-mode.apply()"])
        tbt -. state .-> tba & aba
        end
        subgraph Trunk Heads
        ti((T)) --> tft & tfa
        li((L)) -- L --> tft
        ai((A)) --> afa
        end
        tfa --> tbt & tba
        li --> tbt
        afa --> aba
        subgraph Trunk Tails
        tba --> to((T))
        li -- L --> lo((L))
        aba --> ao((A))
        end


*Composition* is the operation described using the ML
:ref:`workflow expressions <workflow-expression>` based on the individual operators, that allows to
shape the entire task graph in a fully flexible manner.

As shown, the pipeline composition expressions are using the ``>>`` syntax to compose two
operators together. This can be chained further down engaging multiple operators.

The ``.compose()`` method of each operator is receiving the composition *scope* - the upstream
(left) side of the expression - in an *unexpanded* form allowing the ``.compose()`` implementation
to expand it (by calling the ``scope.expand()``) itself as many times as needed.

The *expansion* process triggers the chained ``.compose()`` calls of the upstream operators all
the way up to the *origin* of the given composition *scope*. Explicit scoping can be defined using
the intuitive parenthetical notation. That makes this operation non-associative - e.g. the
expansion scope of operator ``C`` composition in expression ``A >> B >> C`` is the whole
``A >> B``, while in expression ``A >> (B >> C)`` it is just the ``B`` operator.

Further practical details of the composition concept are demonstrated in the :doc:`workflow case
study <study>`.

.. _operator-wrapped:

Wrapped Operators
-----------------

Instead of implementing the entire ``flow.Operator`` base class, operators can in special cases be
defined using the wrappers provided within the ``forml.pipeline.wrap`` package.

This approach is applicable to basic ML entities based on *individual actors* like *transformers*
or *estimators*.


Simple Decorated Operators
^^^^^^^^^^^^^^^^^^^^^^^^^^

Custom actors can be turned into operators easily by wrapping within the provided ``wrap.Operator
.*`` decorators:

.. autoclass:: forml.pipeline.wrap.Operator
   :members: apply, train, label, mapper


.. _operator-autowrap:

Auto-Wrapped Operators
^^^^^^^^^^^^^^^^^^^^^^

Another option for defining particular operators is reusing third-party implementations that are
providing the desired functionality. We've already shown how these entities can be easily
:ref:`mapped into ForML actors <actor-mapped>`. It can, however, be even easier to transparently
*auto-wrap* them directly into ForML operators right upon importing. This can be achieved using
the ``wrap.importer`` context manager:

.. autofunction:: forml.pipeline.wrap.importer


The default list of *auto-wrappers* is available as ``wrap.AUTO`` and contains the following
instances:

* ``wrap.AutoSklearnTransformer>``
* ``wrap.AutoSklearnClassifier``
* ``wrap.AutoSklearnRegressor``

Custom auto-wrappers can be implemented by extending the ``wrap.Auto`` base class:

.. autoclass:: forml.pipeline.wrap.Auto
    :members: match, apply
