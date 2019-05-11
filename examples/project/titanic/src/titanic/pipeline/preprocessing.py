"""
Transformers useful for the Titanic example.

This module is informal from ForML perspective and has been created just for structuring the project code base.

Here we just create couple of forml operators that implement particular transformers.

We demonstrate three different ways of creating a forml operator:
  * Implementing native ForML actor (NanImputer)
  * Creating a wrapped actor from a plain function (parse_title)
  * Wrapping a 3rd party Transformer-like class (ENCODER)
"""
import typing

import category_encoders
import numpy
import pandas

from forml.flow import task
from forml.stdlib.actor import wrapped
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


@simple.Mapper.operator
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


# 3rd party transformer wrapped as an actor into a mapper operator:
ENCODER = simple.Mapper.operator(wrapped.Class.actor(category_encoders.HashingEncoder, train='fit', apply='transform'))
