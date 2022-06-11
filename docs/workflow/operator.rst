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

Operators are the high-level workflow entities used for implementing the actual ML pipeline expressions. They can be
seen as dynamic macro-instructions expanding the particular task graph based on their composition logic. This is a
very powerful concept as it completely abstracts away the internal wiring complexity of the low-level
:doc:`task graph assembly <topology>` providing a simple interface for the pipeline expressions.

If parametrized (rather than hard-coded) with the particular actor implementations, operators can be independent
of the actual data types and formats as they deal purely with the topology. Therefore, many operators
can be shared as library components turning advanced techniques into reusable commodity-like modules (see the ensembler).

Built upon the *pipeline mode duality principle*, operators always deliver the related task graphs for both of
the *train* and *predict* modes together. That's how ForML enforces the train-predict integrity at every step of
the workflow.

The operator layer also happens to be the ideal stage for carrying out unit testing. For this purpose, ForML provides
a complete :doc:`operator unit testing framework <../testing>`.


Implementation
--------------

Operators can implement whatever complex functionality based on any number of actors. They are using the
:ref:`logical topology structures <topology-logical>` to implement the internal task graph and its composition
with the preceding operators.

The base abstract class for implementing operators is the ``flow.Operator``:

.. autoclass:: forml.flow.Operator
   :members: compose, expand


Let's explain the operator development process by implementing a typical *Stateful Mapper* operator. Conceptually,
this operator works as follows:

#. in train-mode:

   #. it first gets *trained* (*Task 1* - ``.train()``) using the train features (via *Train* port) and labels
      (via *Label* port)
   #. then, using the state acquired during the training task, it *maps* (*Task 2* - ``.apply()``) the *train features*
      (via *Apply input* port) producing the transformed output (via *Apply output* port)

#. in apply-mode:

   #. again, using the state acquired during the training task, it *maps* (*Task 3* - ``.apply()``) this time the
      *apply features* (via *Apply input* port) producing the transformed output (via *Apply output* port)

The following diagram outlines the flows:


.. md-mermaid::

    graph LR
        subgraph Mapper Worker Group
        tt("mapper@train-mode.train()")
        ta("mapper@train-mode.apply()")
        aa("mapper@apply-mode.apply()")
        tt -. state .-> ta & aa
        end
        subgraph Trunk Heads
        ti((T)) --> tt & ta
        li((L)) --> tt
        ai((A)) --> aa
        end
        subgraph Trunk Tails
        ta --> to((T))
        aa --> ao((A))
        li --> lo((L))
        end

The segment between the ``A`` head/tail nodes represents the apply-mode task graph, while the segment between
the ``T`` (+ ``L``) nodes represents the train-mode task graph.

Proceeding to the actual implementation, we simply extend the ``flow.Operator`` class and provide the ``.compose()``
method:

.. code-block:: python

    from forml import flow

    class StatefulMapper(flow.Operator):
        """Generic stateful mapper operator."""

        def __init__(self, actor_builder: flow.Builder):
            assert actor_builder.actor.is_stateful(), 'Not stateful'
            self._actor_builder = actor_builder

        def compose(self, left: flow.Composable) -> flow.Trunk:
            preceding: flow.Trunk = left.expand()
            mapper_trainmode_train = flow.Worker(self._actor_builder, 1, 1)
            mapper_trainmode_apply = mapper_trainmode_train.fork()
            mapper_applymode_apply = mapper_trainmode_train.fork()
            mapper_trainmode_train.train(preceding.train.publisher, preceding.label.publisher)
            return preceding.extend(mapper_applymode_apply, mapper_trainmode_apply)


We can see the three workers (forked from the common instance to make them part of the same
:ref:`worker group <topology-state>`) attached to the relevant segments of the preceding trunk. Note the operator
is truly generic as the actual actor implementing the particular mapping function is provided as a parameter.

Given the ``mean_impute`` example actor :ref:`implemented earlier <actor-decorated>`, we can now create two imputation
operators and use them to compose a simple pipeline using the ``>>`` syntax:

.. code-block:: python

    impute_foo = StatefulMapper(mean_impute.builder(column='foo'))
    impute_bar = StatefulMapper(mean_impute.builder(column='bar'))

    pipeline = impute_foo >> impute_bar


That would render the following task graphs:

.. md-mermaid::

    graph LR
        subgraph Foo Worker Group
        tft("foo@train-mode.train()")
        tfa("foo@train-mode.apply()")
        afa("foo@apply-mode.apply()")
        tft -. state .-> tfa & afa
        end
        subgraph Bar Worker Group
        tbt("bar@train-mode.train()")
        tba("bar@train-mode.apply()")
        aba("bar@apply-mode.apply()")
        tbt -. state .-> tba & aba
        end
        subgraph Trunk Heads
        ti((T)) --> tft & tfa
        li((L)) --> tft
        ai((A)) --> afa
        end
        tfa --> tbt & tba
        li --> tbt
        afa --> aba
        subgraph Trunk Tails
        tba --> to((T))
        aba --> ao((A))
        li --> lo((L))
        end


Advanced Composition
--------------------

In the previous sections we've learned, that *composition* is the operation forming the ML pipeline expressions
out of the individual operators in a way that allows to shape the entire task graph in a fully flexible manner.

As shown, the pipeline composition expressions are using the ``>>`` syntax to compose two operators together. This
can be chained further down engaging multiple operators.

The ``.compose()`` method of each operator is getting the *left* (upstream) side of the expression in an *unexpanded*
form allowing the ``.compose()`` implementation to expand it (by calling the ``left.expand()``) itself (as many times
as needed).

The *expansion* process triggers the chained ``.compose()`` calls of the upstream operators all the way up to the
*origin* of the given expression *scope*. Explicit scoping can be defined using the intuitive parenthetical notation.
That makes this operation non-associative - e.g. the expansion scope of operator ``C`` composition in expressions
``A >> B >> C`` is the whole ``A >> B``, while in expression ``A >> (B >> C)`` it is just the ``B`` operator.

To demonstrate the true power of the composition concept, let's implement a more complex operator - we can call it
``KFoldWrapper`` - with the following logic:

#. prepends the train part of the composition scope with a 1:N stateless range-based *splitter* Actor
#. clones the task graph in the composition scope N-times and attaches train part of each clone to the matching
   splitter output port
#. finally sends the apply outputs from all of these N branches to N:1 *reducer* Actor

This can be implemented as follows:

.. code-block:: python

    class KFoldWrapper(flow.Operator):
        """Split-clone-reduce operator for wrapping its composition scope."""

        def __init__(
            self, nfolds: int, splitter_builder: flow.Builder, reducer_builder: flow.Builder
        ):
            assert (
                not splitter_builder.actor.is_stateful()
                and not reducer_builder.actor.is_stateful()
            ), "Is stateful"
            self._nfolds = nfolds
            self._splitter_builder = splitter_builder
            self._reducer_builder = reducer_builder

        def compose(self, left: flow.Composable) -> flow.Trunk:
            apply_head = flow.Future()  # we are going to prepend the entire scope so need some virtual head nodes
            train_head = flow.Future()
            label_head = flow.Future()
            splitter_trainmode_train = flow.Worker(self._splitter_builder, 1, self._nfolds)
            splitter_trainmode_label = splitter_trainmode_train.fork()
            reducer_trainmode_apply = flow.Worker(self._reducer_builder, self._nfolds, 1)
            reducer_applymode_apply = reducer_trainmode_apply.fork()
            splitter_trainmode_train[0].subscribe(train_head[0])
            splitter_trainmode_label[0].subscribe(label_head[0])
            for fid in range(self._nfolds):
                branch = left.expand()  # this will repeatedly expand the scope producing subgraph clone for each fold
                branch.train.subscribe(splitter_trainmode_train[fid])
                branch.label.subscribe(splitter_trainmode_label[fid])
                branch.apply.subscribe(apply_head[0])
                branch.train.prune()  # throw away the train-apply branch as we are going to copy the pre-split apply
                branch_trainmode_apply = branch.apply.copy()  # copy the graph to apply the entire pre-split train data
                branch_trainmode_apply.subscribe(train_head[0])
                reducer_applymode_apply[fid].subscribe(branch.apply.publisher)
                reducer_trainmode_apply[fid].subscribe(branch_trainmode_apply.publisher)
            return flow.Trunk(
                flow.Segment(apply_head, reducer_applymode_apply),
                flow.Segment(train_head, reducer_trainmode_apply),
                flow.Segment(label_head, label_head),  # this will patch through the pre-split labels
            )

Here we have an operator which takes the number of folds and a 1:N splitter actor builder with a N:1 reducer actor
builder. Note how we had to use the :ref:`Future <topology-future>` nodes to create the virtual *heads* of the segments
due to the specific need of prepending


Decorated Operators
^^^^^^^^^^^^^^^^^^^

Standard ML entities like *transformers* or *estimators* can be turned into operators easily by wrapping them within the
provided decorators or adding a provided mixin class into the class hierarchy. More complex entities like for example
a *stacked ensembler* need to be implemented as operators from scratch (reusable entities can be maintained centrally as
library operators). For simple operators (typically single-actor operators) are available convenient decorators under
the ``forml.pipeline.wrap`` that makes it really easy to create specific instances. More details on the
topic of operator development can be found in the :doc:`operator` sections.

Operators are generally defined by implementing the ``flow.Operator`` interface. For couple of trivial patterns, there
also is a simpler option based on decorating user-defined actors (whether native (class based) or decorated):

.. code-block:: python

    import pandas as pd
    from forml.pipeline import wrap

    @wrap.Mapper.operator
    @wrap.Actor.apply
    def impute(df: pandas.DataFrame, *, column: str, value: typing.Any) -> pandas.DataFrame:
        """Simple static imputation actor."""
        return df[column].fillna(value)


Auto-Wrapped Operators
^^^^^^^^^^^^^^^^^^^^^^

Another option of defining actors is reusing third-party classes that are providing the desired functionality. These
classes cannot be changed to extend ForML base Actor class but can be wrapped upon importing using the ``wrap.importer``
context manager like this:

.. code-block:: python

    from forml.pipeline import wrap
    with wrap.importer():
        from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier

    gbc_operator = GradientBoostingClassifier(random_state=42)  # this is now a ForML operator
