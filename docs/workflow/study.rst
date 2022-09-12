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


Workflow Case Study
===================

The following sections demonstrate all the individual :ref:`workflow features <workflow>` engaged
together to implement a practical pipeline. Even though the data actors could be implemented to work
with :ref:`arbitrary data types <actor-compatibility>`, we choose for simplicity the
:class:`pandas:pandas.DataFrame` as our case study payload plus we assume it is all numeric values.


Simple Workflow
---------------

Starting with a basic use case, we want to implement a simple workflow with the following logic:

#. when in the *train-mode*:

   #. applying a custom *binarize* transformation to the input labels
   #. training and applying a custom mean-removal *scaler*
   #. training a :class:`Sklearn LogisticRegression
      <sklearn:sklearn.linear_model.LogisticRegression>` classifier

#. when in the *apply-mode*:

   #. applying the trained *scaler* transformer
   #. making a prediction with the :class:`Sklearn LogisticRegression
      <sklearn:sklearn.linear_model.LogisticRegression>` classifier


We are going to implement the :ref:`actors <actor-decorated>` and :ref:`operators
<operator-wrapped>` with the help of the decorator wrappers:


.. code-block:: python
    :linenos:

    import pandas
    import typing
    from forml.pipeline import wrap

    with wrap.importer():
        # sklearn LogisticRegression auto-wrapped as ForML operator
        from sklearn.linear_model import LogisticRegression


    @wrap.Operator.label
    @wrap.Actor.apply
    def Binarizer(labels: pandas.Series, *, threshold: float = 0.0) -> pandas.Series:
        """Stateless actor wrapped into a *label* transforming operator."""
        labels[labels > threshold] = 1
        labels[labels <= threshold] = 0
        return labels


    @wrap.Actor.train
    def Scaler(
        mean: typing.Optional[pandas.Series],
        features: pandas.DataFrame,
        labels: pandas.Series,
    ) -> pandas.Series:
        """Train part of a simple scaler (mean removal) actor."""
        return features.mean()


    @wrap.Operator.mapper
    @Scaler.apply
    def Scaler(mean: pandas.Series, features: pandas.DataFrame) -> pandas.DataFrame:
        """Apply part of a simple scaler (mean removal) actor wrapped as a mapper operator."""
        return features - mean


    PIPELINE = Binarizer() >> Scaler() >> LogisticRegression()


This straightforward implementation produces a ``PIPELINE`` represented using a :class:`flow.Trunk
<forml.flow.Trunk>` with the following visualization:

.. md-mermaid::

    flowchart TD
        subgraph Train Mode
            btl(["Binarizer.apply()"]) -- L --> stt["Scaler.train()"] & ltt["LogisticRegression.train()"]
            sta(["Scaler.apply()"]) --> ltt
            stt -. state .-> sta
        end
        subgraph Apply Mode
            saa(["Scaler.apply()"]) --> laa(["LogisticRegression.apply()"])
            stt -. state .-> saa
            ltt -. state .-> laa
        end
        subgraph Trunk Heads
            ti((T)) --> stt & sta
            li((L)) -- L --> btl
            ai((A)) --> saa
        end
        subgraph Trunk Tails
            sta --> to((T))
            btl -- L --> lo((L))
            laa --> ao((A))
        end


Complex Operator
----------------

To demonstrate the true power of the :ref:`composition concept <operator-composition>`, let's
implement a more complex operator - we can call it ``KFoldWrapper`` - with the following logic:

#. prepends the train part of the composition scope with a 1:N stateless range-based *splitter*
   Actor
#. clones the task graph in the composition scope N-times and with each of its train segments:

   #. attach the head to the matching *splitter* output port
   #. attach the tail to the matching *stacker* input port

#. finally sends the apply-mode outputs from all of these N branches to the N:1 *reducer* Actor

The idea behind this operator is to *train+apply* the preceding scope in multiple parallel
instances on the range-split part of the data and stacking these partial results back together in
the *train-mode* using the *stacker* while reducing them into a single value using the *reducer*
when in the *apply-mode*.

Such an operator can be implemented by extending the :class:`flow.Operator <forml.flow.Operator>` as
follows:

.. code-block:: python
    :linenos:

    from forml import flow

    class KFoldWrapper(flow.Operator):
        """Split-clone-reduce operator for wrapping its composition scope."""

        def __init__(
            self,
            nfolds: int,
            splitter_builder: flow.Builder,
            stacker_builder: flow.Builder,
            reducer_builder: flow.Builder,
        ):
            assert not (
                splitter_builder.actor.is_stateful()
                or stacker_builder.actor.is_stateful()
                or reducer_builder.actor.is_stateful()
            ), "Stateless expected"
            self._nfolds = nfolds
            self._splitter_builder = splitter_builder
            self._stacker_builder = stacker_builder
            self._reducer_builder = reducer_builder

        def compose(self, scope: flow.Composable) -> flow.Trunk:
            apply_head = flow.Future()  # virtual head nodes to prepend the entire scope
            label_head = flow.Future()
            splitter_trainmode_train = flow.Worker(self._splitter_builder, 1, self._nfolds)
            splitter_trainmode_label = splitter_trainmode_train.fork()
            stacker_trainmode_apply = flow.Worker(self._stacker_builder, self._nfolds, 1)
            reducer_applymode_apply = flow.Worker(self._reducer_builder, self._nfolds, 1)
            splitter_trainmode_label[0].subscribe(label_head[0])
            for fid in range(self._nfolds):
                # repeatedly expand the scope producing subgraph clone for each fold
                branch = scope.expand()
                branch.train.subscribe(splitter_trainmode_train[fid])
                branch.label.subscribe(splitter_trainmode_label[fid])
                branch.apply.subscribe(apply_head[0])
                reducer_applymode_apply[fid].subscribe(branch.apply.publisher)
                stacker_trainmode_apply[fid].subscribe(branch.train.publisher)
            return flow.Trunk(
                flow.Segment(apply_head, reducer_applymode_apply),
                flow.Segment(splitter_trainmode_train, stacker_trainmode_apply),
                flow.Segment(label_head, label_head),  # patch through the pre-split labels
            )

Note how it uses the :ref:`Future <topology-future>` nodes to create the virtual *heads* for
some of its segments to prepend the entire composition scope. In each
iteration, the ``for`` loop expands the *left* side of the composition scope producing the branch
task graph to be wrapped. Its *train* and *label* input segments get attached to the relevant
splitter ports, while the *apply* segment goes directly to the main *apply-mode* head node.


Final Pipeline
--------------

Let's now upgrade our pipeline expression with this operator to demonstrate the full
composition functionality. For complete illustration, we also provide the possible
implementations of ``Splitter`` and ``Mean`` actors.

.. code-block:: python
    :linenos:

    import math
    from forml.pipeline import payload

    @wrap.Actor.apply
    def Splitter(
        features: pandas.DataFrame,
        *,
        nfolds: int
    ) -> typing.Sequence[pandas.DataFrame]:
        """1:N range based splitter actor."""
        chunk = math.ceil(len(features) / nfolds)
        return [
            features.iloc[i:i + chunk].reset_index(drop=True)
            for i in range(0, nfolds * chunk, chunk)
        ]


    @wrap.Actor.apply
    def Mean(*folds: pandas.DataFrame) -> pandas.DataFrame:
        """N:1 mean based reducer actor."""
        full = pandas.concat(folds, axis='columns', copy=False)
        return full.groupby(by=full.columns, axis='columns').mean()


    kfold_wrapper = KFoldWrapper(
        2,
        Splitter.builder(nfolds=2),
        payload.PandasConcat.builder(axis='index', ignore_index=True),
        Mean.builder(),
    )

    PIPELINE = Binarizer() >> (Scaler() >> kfold_wrapper) >> LogisticRegression()


We deliberately chose (by applying the parentheses) the :ref:`composition scope
<operator-composition>` to include just the preceding ``Scaler`` operator without the ``Binarizer``.
For the readability of the following visualization, we set the ``nfolds`` (which results in the
number of branches) to just ``2``. That leads to the following diagram:

.. md-mermaid::

    flowchart TD
        subgraph Train Mode
            btl(["Binarizer.apply()"]) -- L --> ftl(["Splitter[L].apply()"]) & ltt["LogisticRegression.train()"]
            fta(["Splitter[F].apply()"]) -- F1 --> s1tt["Scaler[1].train()"] & s1ta(["Scaler[1].apply()"])
            fta -- F2 --> s2tt["Scaler[2].train()"] & s2ta(["Scaler[2].apply()"])
            ftl -- L1 --> s1tt
            ftl -- L2 --> s2tt
            s1ta & s2ta --> cta
            cta(["Concat.apply()"]) --> ltt
            s1tt -. state .-> s1ta
            s2tt -. state .-> s2ta
        end
        subgraph Apply Mode
            s1aa(["Scaler[1].apply()"]) & s2aa(["Scaler[2].apply()"]) --> raa(["Mean.apply()"])
            raa --> laa(["LogisticRegression.apply()"])
            ltt -. state .-> laa
            s1tt -. state .-> s1aa
            s2tt -. state .-> s2aa
        end
        subgraph Trunk Heads
            ti((T)) --> fta
            li((L)) -- L --> btl
            ai((A)) --> s1aa & s2aa
        end
        subgraph Trunk Tails
            btl -- L --> lo((L))
            cta --> to((T))
            laa --> ao((A))
        end

As you can see, there remains to be a single instance of the ``Binarizer`` as well as the
``LogisticRegression`` classifier, while the inner part of the task graph now forks into
two branches and merges back together by ``Concat`` in the *train-mode* (where each branch receives
distinct train data) and ``Mean`` in the *apply-mode* (where the branches receive the same data).
