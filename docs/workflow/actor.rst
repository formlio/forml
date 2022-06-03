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

Task Actor
==========

Actor is the lowest level entity - a node in the task graph - representing an atomic black box transformation of the
passing data.

.. important::
    ForML doesn't care neither about the particular internal processing functionality of any actors nor the
    actual types and formats of the data passed between them. All that ForML deals with is the actor interconnection
    within the overall flow topology - responsibility for their logical and functional compatibility is solely in hands
    of the implementor.

The two main actor types are:

#. Plain *stateless* actors which define output as a function applied just to their input.
#. More complex *stateful* actors produce output based on not just the input but also their inner *state* which it
   acquires during separate phase called *train*. We then distinguish between the *train-mode* and *apply-mode* in which
   the stateful actors operate.


.. _actor-ports:

Ports
-----

ForML actors have number of input and output *ports* for the mutual :doc:`interconnection <topology>` within the task
graph. The following diagram shows how the different ports might get engaged when in each of the particular actor modes:

.. md-mermaid::

    graph LR
        subgraph Actor
        A[/apply-mode/]
        T[\train-mode\]
        end
        subgraph Input Ports
        AI1[/Apply 1\] --> A
        AI2[/Apply ...\] --> A
        AIM[/Apply M\] --> A
        TI[\Train/] --> T
        LI[\Labels/] --> T
        SI[(State)] -. set .-> A & T
        PI>Params] -. set .-> A & T
        end
        subgraph Output Ports
        A --> AO1[/Apply 1\]
        A --> AO2[/Apply ...\]
        A --> AON[/Apply N\]
        T -. get .-> SO[(State)]
        A & T -. get .-> PO>Params]
        end


There is a couple of different ways the ports can logically be grouped together:

**Level** - how are the ports configured:
    * *user* level ports (full lines in diagram) are explicitly connected by the implementor
    * *system* level ports (dotted lines in diagram) are internally managed exclusively by ForML

**Mode** - when do the ports get engaged:
    * *train* mode ports are engaged only during the train-mode
    * *apply* mode ports are engaged only during the apply-mode
    * *both* mode ports can be engaged in any mode

**Direction** - which way the data flows through the ports:
    * *input* ports are passing data into the Actor
    * *output* ports are emitting data out of the Actor

With this perspective, we can now describe each of the different ports as follows:

======  ======  =====  ========  =========  ===============================================================
 Name   Level   Mode   # Inputs  # Outputs  Description
======  ======  =====  ========  =========  ===============================================================
Apply    user   apply      M         N      The main featureset ports(s) to/from the apply-transformation.
Train    user   train      1         0      Features port to be trained on.
Label    user   train      1         0      Labels port to be trained on.
State   system  both       1         1      State getter/setter ports.
Params  system  both       1         1      Hyper-parameter getter/setter ports.
======  ======  =====  ========  =========  ===============================================================

.. seealso::

    The actual port management is discussed in great detail in the :doc:`topology` chapter, here we stay focused rather
    on the Actor itself.


API
---

The actor API is defined using an abstract class of ``flow.Actor``. The generic way of implementing user-defined actors
is to simply extend this class providing the relevant methods with the desired functionality. The main parts of the API
look as follows:

.. autoclass:: forml.flow.Actor
   :members: apply, train, get_state, set_state, get_params, set_params


.. _actor-spec:

.. autoclass:: forml.flow.Spec


Implementation
--------------

The following sections explain the different ways an Actor can be implemented.

Native Actors
^^^^^^^^^^^^^

The basic mechanism for declaring custom actors is simply extending the ``flow.Actor`` base class.

Example of a user-defined native actor:

.. md-tab-set::

    .. md-tab-item:: Stateless Actor

        .. code-block:: python

            import typing
            import pandas as pd
            from forml import flow

            class StaticImpute(flow.Actor[pd.DataFrame, None, pd.DataFrame]):
                """Simple stateless imputation actor using the provided value to fill the NaNs."""

                def __init__(self, column: str, value: float):
                    self._column: str = column
                    self._value: float = value

                def apply(self, df: pd.DataFrame) -> pd.DataFrame:
                    return df[self._column].fillna(self._value)

                def get_params(self) -> typing.Mapping[str, typing.Any]:
                    return {'column': self._column, 'value': self._value}

                def set_params(self, column: typing.Optional[str] = None, value: typing.Optional[float] = None) -> None:
                    if column is not None:
                        self._column = column
                    if value is not None:
                        self._value = value

    .. md-tab-item:: Stateful Actor

        .. code-block:: python

            import typing
            import pandas as pd
            from forml import flow

            class MeanImpute(flow.Actor[pd.DataFrame, pd.Series, pd.DataFrame]):
                """Simple stateful imputation actor using the trained mean value to fill the NaNs.

                Using the default implementations of ``.get_state()`` and ``.set_state()`` methods.
                """

                def __init__(self, column: str):
                    self._column: str = column
                    self._value: typing.Optional[float] = None

                def train(self, df: pd.DataFrame, labels: pd.Series) -> None:
                    self._value = df[self._column].mean()

                def apply(self, df: pd.DataFrame) -> pd.DataFrame:
                    if self._value is None:
                        raise RuntimeError('Not trained')
                    df[self._column] = df[self._column].fillna(self._value)
                    return df

                def get_params(self) -> typing.Mapping[str, typing.Any]:
                    return {'column': self._column}

                def set_params(self, column: str) -> None:
                    self._column = column


Decorated Function Actors
^^^^^^^^^^^^^^^^^^^^^^^^^

Less verbose option for defining actors is based on wrapping user-defined functions using the ``@wrap.Actor.train``
and/or ``@wrap.Actor.apply`` decorators from the :doc:`Pipeline Library <../pipeline>` (the following examples match
exactly the functionality as in the native implementations above):

.. md-tab-set::

    .. md-tab-item:: Stateless Actor

        .. code-block:: python

            import typing
            import pandas as pd
            from forml.pipeline import wrap

            @wrap.Actor.apply
            def static_impute(df: pd.DataFrame, *, column: str, value: float) -> pd.DataFrame:
                """Simple stateless imputation actor using the provided value to fill the NaNs."""
                df[column] = df[column].fillna(value)
                return df

    .. md-tab-item:: Stateful Actor

        .. code-block:: python

            import typing
            import pandas as pd
            from forml.pipeline import wrap

            @wrap.Actor.train
            def mean_impute(
                state: typing.Optional[float],
                df: pd.DataFrame,
                labels: pd.Series,
                *,
                columns: str,
            ) -> float:
                """Train part of a stateful imputation actor using the trained mean value to fill the NaNs."""
                return df[column].mean()

            @mean_impute.apply
            def mean_impute(state: float, df: pd.DataFrame, *, column: str) -> pd.DataFrame:
                """Apply part of a stateful imputation actor using the trained mean value to fill the NaNs."""
                df[column] = df[column].fillna(value)
                return df


Mapped Actors
^^^^^^^^^^^^^

Third-party implementations that are logically compatible with the ForML actor concept can be easily mapped into a valid
ForML actors using the ``@wrap.Actor.type`` wrapper from the :doc:`Pipeline Library <../pipeline>`:

.. code-block:: python

    from sklearn import ensemble
    from forml.pipeline import wrap

    RFC_ACTOR = wrap.Actor.type(
        ensemble.RandomForestClassifier,
        train='fit',  # mapping using target method reference
        apply=lambda c, *a, **kw: c.predict_proba(*a, **kw).transpose()[-1],  # mapping using a callable wrapper
    )


.. attention::
    Rather then to just Actors, the third-party implementations are usually required to be converted all the way to
    ForML :doc:`operators <operator>` to be eventually composable within the pipeline expressions. For this purpose,
    there is even easier method of turning those implementations into operators with no effort using the
    ``@wrap.importer`` context manager - see ...
