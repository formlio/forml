Pipeline is the backbone of the ML solution responsible for holding all its pieces in the right place together. On the
low level it is a _Task Dependency Graph_ where edges represent data flows and vertices are the data transformations.

ForML is providing a convenient API for defining complex Pipelines using simple notation based on two main pipeline
entities:
* _Operators_ that can be seen as pipeline plugins implementing specific ML techniques.
* _Actors_ as the low level primitives forming the graph vertices.

Each ForML pipeline has dual _train_ vs _apply_ mode for implementing the specific scenarios of supervised learning.

The high-level API for describing a pipeline allows to formulate an operator composition expressions using a syntax
like this: 

```python

flow = LabelExtractor(column='foo') >> NaNImputer() >> RFC(max_depth=3)
```

Given the particular implementation of the example operators this will render a pipeline with the _train_ and _apply_
graphs visualized as follows:

![Pipeline DAGs](pipeline.png)

The meaning of operators and how are they defined using actors is described in more details in the following sections.

Actor
-----

Actor is the lowest level task graph entity representing an atomic blackbox with three types of _application ports_:
* M input and N output _Apply_ ports
* one _Train_ input port
* one _Label_ input port

Additionally there are two types of _system ports_ but they are not available for manipulation from the user API.
These are:
* one input and output _State_ port
* one input and output _Params_ port (hyper parameters) 

Actor is expected to process data arriving to input ports and return results using output ports if applicable. There is
specific consistency constraint which ports can or need to be active (attached) at the same time: either both _Train_
and _Label_ or all _Apply_ inputs and outputs.

Ports of different actors can be connected via subscriptions. Any input port can be subscribed to at most one upstream
output port but any output port can be publishing to multiple subscribed input ports. Actor cannot be subscribed to
itself.

The system doesn't care what is the particular internal processing functionality of any actors, all that matters is
their interconnection determining the task graph topology.

The actor API is defined using an abstract class of `forml.flow.task.Actor`. For user defined actors it's best to
simply extend this class filling in the abstract methods with the desired functionality. The meaning of these methods
is:

* `apply(features: typing.Union[DataT, typing.Sequence[DataT]]) -> typing.Union[DataT, typing.Sequence[DataT]]` - 
mandatory M:N input-output `Apply` ports 
* `train(features: TableT, label: VectorT) -> None` - optional method engaging the _Train_ (`features`) and _Label_
(`label`) ports on stateful actors
* `get_params() -> typing.Dict[str, typing.Any]` and `set_params(params: typing.Dict[str, typing.Any]) -> None` -
mandatory input and output `Params` ports

Example of user-defined actor:

```python
import typing
import pandas
from forml.flow import task

class LabelExtractor(task.Actor[pandas.DataFrame]):
    """Simple label-extraction actor returning a specific column from input feature set.
    """
    def __init__(self, column: str = 'label'):
        self._column: str = column
    
    def apply(self, features: pandas.DataFrame) -> pandas.DataFrame:
        """Label extraction logic.
        """
        return features[0][[self._column]]

    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Mandatory get params.
        """
        return {'column': self._column}

    def set_params(self, params: typing.Dict[str, typing.Any]) -> None:
        """Mandatory set params.
        """
        self._column = params.get('column', self._column)
```

Note this actor doesn't implement the `train` method making it a simple _stateless_ actor.

Another option of defining actors is reusing third-party classes that are providing desired functionality. These classes
cannot be changed to extend ForML base Actor class but can be wrapped using a ``forml.flow.task.Actor.Wrapped.actor`
decorator like this:

```python
from sklearn import ensemble as sklearn
from forml.flow import task

gbc_actor = task.Wrapped.actor(sklearn.GradientBoostingClassifier, train='fit', apply='predict_proba')
```

Note the extra parameters used to map the third-party class methods to the expected Actor API methods.


Operator
--------

Operators represent the high-level abstraction of the task dependency graph. They are built using one or more actors
and support a _composition operation_ (the `>>` syntax) for building up the pipeline. Each operator defines its actors
and their wiring and expands the task graph through composition with other operators.

To implement the pipeline mode duality operators actually define the composition separately for each of the two modes.
This eventually allows to produce different graph topology for _train_ vs _apply_ mode while defining the pipeline
just once using one set of operators. This also prevents any inconsistencies between the _train_ vs _apply_ flows as
these are only assembled along each other when composing the encapsulating operators.

Operators can implement whatever complex functionality using any number of actors. There is however one condition: the
subgraph defined by an operator can internally split into multiple branches but can only be connected (both on input and
output side) to other operators using single port of single node.

For simple operators (typically single-actor oprtators) like transformers or estimators are available convenient
decorators under the `forml.flow.operator.simple` that make it really easy to create specific instances.

Following is an example of creating simple transformer operator by decorating an user defined actor with the
`simple.Mapper.operator` decorator:

```python
import typing
import pandas
import numpy
from forml.flow import task
from forml.flow.operator import simple

@simple.Mapper.operator
class NaNImputer(task.Actor[pandas.DataFrame]):
    """Custom NaN imputation logic.
    """
    def train(self, features: pandas.DataFrame, label: pandas.DataFrame):
        """Impute missing values using the median for numeric columns and the most common value for string columns.
        """
        self._fill = pandas.Series([features[f].value_counts().index[0] if features[f].dtype == numpy.dtype('O')
                                    else features[f].median() for f in features], index=features.columns)
        return self

    def apply(self, features: pandas.DataFrame) -> pandas.DataFrame:
        """Filling the NaNs.
        """
        return features.fillna(self.fill)

    def get_params(self) -> typing.Dict[str, typing.Any]:
        """Mandatory get params.
        """
        return {}

    def set_params(self, params: typing.Dict[str, typing.Any]) -> None:
        """Mandatory set params.
        """
        pass
```

It is also possible to use the decorator to create operators from third-party wrapped Actors:

```python
from sklearn import ensemble as sklearn
from forml.flow import task
from forml.flow.operator import simple

RFC = simple.Consumer.operator(task.Wrapped.actor(sklearn.RandomForestClassifier, train='fit', apply='predict_proba'))
```

These operators are now good to be used for pipeline composition.