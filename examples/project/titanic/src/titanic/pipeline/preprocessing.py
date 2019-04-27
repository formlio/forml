"""Transformers useful for the Titanic example.
"""
import typing

import category_encoders
import numpy as np
import pandas
import pandas as pd
from sklearn import base as skbase

from forml.flow import task
from forml.stdlib import actor
from forml.stdlib.operator import simple


class NaNImputer(task.Actor):
    """Imputer for missing values implemented as native forml actor.
    """
    def __init__(self):
        self._fill: typing.Optional[pandas.Series] = None

    def train(self, data: pandas.DataFrame, label: pandas.Series) -> None:
        """Method required by the Sklearn API - fit.

        Impute missing values using the median for numeric columns and
        the most common value for string columns.
        """
        self._fill = pd.Series([data[c].value_counts().index[0] if data[c].dtype == np.dtype('O')
                               else data[c].median() for c in data], index=data.columns)

    def apply(self, data: pandas.DataFrame) -> pandas.DataFrame:
        """Method required by the Sklearn API - transform.
        """
        return data.fillna(self._fill)


NANIMPUTER = simple.Mapper.operator(NaNImputer)


class TitleParser(skbase.TransformerMixin):
    """Transformer extracting a person's title from the name string implemented as scikit-learn compatible transformer.
    """
    def __init__(self, source: str = 'name', target: str = 'title'):
        self.source = source
        self.target = target

    @staticmethod
    def get_title(name: str) -> str:
        """Auxiliary method for extracting the title.
        """
        if '.' in name:
            return name.split(',')[1].split('.')[0].strip()
        return 'Unknown'

    def transform(self, data: pandas.DataFrame) -> pandas.DataFrame:
        """Method required by the Sklearn API - transform.
        """
        data[self.target] = data[self.source].map(self.get_title)
        return data

    def get_params(self) -> typing.Mapping[str, str]:
        """Method required by the Sklearn API - get_params.
        """
        return dict(self.__dict__)

    def set_params(self, source: str = 'name', target: str = 'title'):
        """Method required by the Sklearn API - set_params.
        """
        self.source = source
        self.target = target


TITLEPARSER = simple.Mapper.operator(actor.Wrapped.actor(TitleParser, apply='transform'))
ENCODER = simple.Mapper.operator(actor.Wrapped.actor(category_encoders.HashingEncoder, train='fit', apply='transform'))
