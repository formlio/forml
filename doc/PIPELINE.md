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

* `apply(*features: DataT) -> typing.Union[DataT, typing.Sequence[DataT]]` - 
mandatory M:N input-output `Apply` ports 
* `train(features: DataT, label: DataT) -> None` - optional method engaging the _Train_ (`features`) and _Label_
(`label`) ports on stateful actors
* `get_params() -> typing.Dict[str, typing.Any]` and `set_params(params: typing.Dict[str, typing.Any]) -> None` -
mandatory input and output `Params` ports


### Native Actors

Basic mechanism for declaring custom actors is implementing the `task.Actor` interface.

Example of user-defined native actor:

```python
import typing
import pandas
from forml.flow import task

class LabelExtractor(task.Actor):
    """Simple label-extraction actor returning a specific column from input feature set.
    """
    def __init__(self, column: str = 'label'):
        self._column: str = column

    def apply(self, features: pandas.DataFrame) -> typing.Tuple[pandas.DataFrame, pandas.Series]:
        return features.drop(columns=self._column), features[self._column]

    def get_params(self) -> typing.Dict[str, typing.Any]:
        return {'column': self._column}

    def set_params(self, column: str) -> None:
        self._column = column
```

Note this actor doesn't implement the `train` method making it a simple _stateless_ actor.


### Wrapped Class Actors

Another option of defining actors is reusing third-party classes that are providing desired functionality. These classes
cannot be changed to extend ForML base Actor class but can be wrapped using a `forml.stdlib.actor.wrapped.Class.actor`
decorator like this:

```python
from sklearn import ensemble as sklearn
from forml.stdlib.actor import wrapped

gbc_actor = wrapped.Class.actor(sklearn.GradientBoostingClassifier, train='fit', apply='predict_proba')
```

Note the extra parameters used to map the third-party class methods to the expected Actor API methods.


### Decorated Function Actors

Last option of defining actors is simplistic decorating of user-defined functions:

```python
from forml.stdlib.actor import wrapped

@wrapped.Function.actor
def parse_title(data: pandas.DataFrame, source: str, target: str) -> pandas.DataFrame:
    """Transformer extracting a person's title from the name string implemented as wrapped stateless function.
    """
    def get_title(name: str) -> str:
        """Auxiliary method for extracting the title.
        """
        if '.' in name:
            return name.split(',')[1].split('.')[0].strip()
        return 'Unknown'

    data[target] = data[source].map(get_title)
    return data
```


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

For simple operators (typically single-actor operators) like _transformers_ or _estimators_ are available convenient
decorators under the `forml.flow.operator.simple` that make it really easy to create specific instances.

Following is an example of creating simple transformer operator by decorating an user defined actor with the
`simple.Mapper.operator` decorator:

```python
import typing
import pandas
import numpy
from forml.flow import task
from forml.stdlib.operator import simple

@simple.Mapper.operator
class NaNImputer(task.Actor):
    """Imputer for missing values implemented as native ForML actor.
    """
    def __init__(self):
        self._fill: typing.Optional[pandas.Series] = None

    def train(self, data: pandas.DataFrame, label: pandas.Series) -> None:
        """Train the actor by learning the median for each numeric column and finding the most common value for strings.
        """
        self._fill = pandas.Series([data[c].value_counts().index[0] if data[c].dtype == numpy.dtype('O')
                                    else data[c].median() for c in data], index=data.columns)

    def apply(self, data: pandas.DataFrame) -> pandas.DataFrame:
        """Apply the imputation to the given dataset.
        """
        return data.fillna(self._fill)
```

It is also possible to use the decorator to create operators from third-party wrapped Actors:

```python
from sklearn import ensemble as sklearn
from forml.stdlib import actor
from forml.stdlib.operator import simple

RFC = simple.Consumer.operator(actor.Wrapped.actor(sklearn.RandomForestClassifier, train='fit', apply='predict_proba'))
```

These operators are now good to be used for pipeline composition.
