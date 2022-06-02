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
----------------------

Another option of defining actors is reusing third-party classes that are providing the desired functionality. These
classes cannot be changed to extend ForML base Actor class but can be wrapped upon importing using the ``wrap.importer``
context manager like this:

.. code-block:: python

    from forml.pipeline import wrap
    with wrap.importer():
        from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier

    gbc_operator = GradientBoostingClassifier(random_state=42)  # this is now a ForML operator
