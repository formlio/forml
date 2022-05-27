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

Path & Trunk API
================

Workflow is the backbone of the ML solution responsible for holding all its pieces in the right place together. On the
low level, it is a *Task Dependency Graph* where edges represent data flows and vertices are the data transformations.

ForML is providing a convenient API for defining complex workflows using simple notation based on two main entities:

Operators
    are high-level pipeline macros that can be composed together and eventually expand into the task graph.
Actors
    as the low-level primitives forming the graph vertices.

Each ForML workflow has dual *train* vs *apply* mode for implementing the specific scenarios of supervised learning.

The high-level API for describing a pipeline allows to formulate an operator composition expressions using a syntax
like this::

    flow = LabelExtractor(column='foo') >> NaNImputer() >> RFC(max_depth=3)

Given the particular implementation of the example operators, this will render a pipeline with the *train* and *apply*
graphs visualized as follows:

.. image:: _static/images/workflow.png

The meaning of operators and how are they defined using actors is described in more details in the following sections.

Actor
-----

Actor is the lowest level task graph entity representing an atomic black box with three types of *application ports*:

* M input and N output *Apply* ports
* one *Train* input port
* one *Label* input port

Additionally, there are two types of *system ports* but they are not available for manipulation from the user API.
These are:

* one input and output *State* port
* one input and output *Params* port (hyperparameters)

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
.............

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
.........................

More convenient option for defining actors is based on decorating user-defined functions::

    import pandas as pd
    from forml.pipeline import wrap

    @wrap.Actor.apply
    def impute(df: pandas.DataFrame, *, column: str, value: typing.Any) -> pandas.DataFrame:
        """Simple static imputation actor."""
        return df[column].fillna(value)


Operator
--------

Operators represent the high-level abstraction of the task dependency graph. They are built using one or more actors
and support a *composition operation* (the ``>>`` syntax) for building up the particular pipeline. Each operator defines
its actors and their wiring and expands the task graph through composition with other operators.

Operator composition is a very powerful concept built into ForML. It is the composition in
the `mathematical sense <https://en.wikipedia.org/wiki/Function_composition>`_ that allows expanding the task graph
topology into a complex layout just by a simple combination of two operators. More details about the composition
mechanism are discussed in the :doc:`operator` sections.

Pipeline for supervised learning project has typically two modes - *learning* and *applying* (also known as *training*
or *fitting* and *predicting* or *transforming*). To implement the pipeline mode duality, operators actually define
the composition separately for each of the two modes. This eventually allows producing different graph topology for
*train* vs *apply* mode while defining the pipeline just once using one set of operators. This also prevents any
inconsistencies between the *train* vs *apply* flows as these are only assembled along each with other when composing
the encapsulating operators.

Operators can implement whatever complex functionality using any number of actors. There is however one condition: the
subgraph defined by an operator can internally split into multiple branches but can only be connected (both on input and
output side) to other operators using a single port of a single node.

Standard ML entities like *transformers* or *estimators* can be turned into operators easily by wrapping them within the
provided decorators or adding a provided mixin class into the class hierarchy. More complex entities like for example
a *stacked ensembler* need to be implemented as operators from scratch (reusable entities can be maintained centrally as
library operators). For simple operators (typically single-actor operators) are available convenient decorators under
the ``forml.pipeline.wrap`` that makes it really easy to create specific instances. More details on the
topic of operator development can be found in the :doc:`operator` sections.

Operators are generally defined by implementing the ``flow.Operator`` interface. For couple of trivial patterns, there
also is a simpler option based on decorating user-defined actors (whether native (class based) or decorated)::

    import pandas as pd
    from forml.pipeline import wrap

    @wrap.Mapper.operator
    @wrap.Actor.apply
    def impute(df: pandas.DataFrame, *, column: str, value: typing.Any) -> pandas.DataFrame:
        """Simple static imputation actor."""
        return df[column].fillna(value)


Auto-Wrapped Operators
......................

Another option of defining actors is reusing third-party classes that are providing the desired functionality. These
classes cannot be changed to extend ForML base Actor class but can be wrapped upon importing using the ``wrap.importer``
context manager like this::

    from forml.pipeline import wrap
    with wrap.importer():
        from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier

    gbc_operator = GradientBoostingClassifier(random_state=42)  # this is now a ForML operator
