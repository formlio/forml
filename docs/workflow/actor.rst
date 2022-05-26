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

ForML actors have number of input and output *ports* for the mutual :doc:`interconnection <topology>` within the task
graph. The following diagram shows how the different ports get engaged in the particular actor mode:

.. md-mermaid::
    :name: flowcharts

    graph LR
        subgraph Actor
        A[/Apply/]
        T[\Train\]
        end
        subgraph Input Ports
        AI1[/apply 1\] --> A
        AI2[/apply ...\] --> A
        AIM[/apply M\] --> A
        TI[\train/] --> T
        LI[\labels/] --> T
        SI[(state)] -. set .-> A & T
        PI>params] -. set .-> A & T
        end
        subgraph Output Ports
        A --> AO1[/apply 1\]
        A --> AO2[/apply ...\]
        A --> AON[/apply N\]
        T -. get .-> SO[(state)]
        end




There are three types of *application ports* :

+------------------------+-----------------------+--------------------------------+
|                        |    Port Quantities    |                                |
|      Port Type         +------------+----------+         API signature          |
|                        |   Inputs   | Outputs  |                                |
+========================+============+==========+================================+
|                        |            |          | .. code-block:: python         |
|      Apply             |      M     |     N    |                                |
|                        |            |          |     .apply(*inputs) -> outputs |
+------------------------+------------+----------+--------------------------------+
|                        |            |          | .. code-block:: python         |
|      Train             |      1     |     0    |                                |
|                        |            |          |     .train(features, labels)   |
+------------------------+------------+----------+                                |
|                        |            |          |                                |
|      Label             |      1     |     0    |                                |
|                        |            |          |                                |
+------------------------+------------+----------+--------------------------------+


Additionally, there are two types of *system ports* not accessible from the user API.
These are:

=========  =======  ========
Port Type  Input #  Output #
=========  =======  ========
State      1        1
Params     1        1
=========  =======  ========

Actor is expected to process data arriving to input ports and return results using output ports if applicable. There
is specific consistency constraint which ports can or need to be active (attached) at the same time: either both *Train*
and *Label* or all *Apply* inputs and outputs.

Ports of different actors can be connected via subscriptions. Any input port can be subscribed to at most one upstream
output port but any output port can be publishing to multiple subscribed input ports. Actor cannot be subscribed to
itself.

The system doesn't care what is the particular internal processing functionality of any actors, all that matters is
their interconnection determining the task graph topology.

The actor API is defined using an abstract class of ``flow.Actor``. For user-defined actors it's best to
simply extend this class filling in the abstract methods with the desired functionality. The API looks like this:

.. autoclass:: forml.flow.Actor
   :members: apply, train, get_params, set_params, get_state, set_state


Native Actors
-------------

The basic mechanism for declaring custom actors is implementing the ``flow.Actor`` interface.

Example of a user-defined native actor::

    import typing
    import pandas as pd
    from forml import flow

    class Impute(flow.Actor):
        """Simple static imputation actor."""

        def __init__(self, column: str, value: typing.Any):
            self._column: str = column
            self._value: typing.Any = value

        def apply(self, df: pd.DataFrame) -> typing.Tuple[pd.DataFrame, pd.Series]:
            return df[self._column].fillna(self._value)

        def get_params(self) -> typing.Mapping[str, typing.Any]:
            return {'column': self._column, 'value': self._value}

        def set_params(self, column: typing.Optional[str] = None, value: typing.Optional[str] = None) -> None:
            if column is not None:
                self._column = column
            if value is not None:
                self._value = value

Note this actor doesn't implement the ``train`` method making it a simple *stateless* actor.


Decorated Function Actors
-------------------------

More convenient option for defining actors is based on decorating user-defined functions::

    import pandas as pd
    from forml.pipeline import wrap

    @wrap.Actor.apply
    def impute(df: pandas.DataFrame, *, column: str, value: typing.Any) -> pandas.DataFrame:
        """Simple static imputation actor."""
        return df[column].fillna(value)
