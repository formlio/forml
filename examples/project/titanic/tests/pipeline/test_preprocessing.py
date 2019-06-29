"""
Titanic preprocessing unit tests.
"""
# pylint: disable=no-self-use
import numpy as np
import pandas as pd
import pytest
from forml.flow import task

from titanic.pipeline import preprocessing


class Transformer:  # pylint: disable=too-few-public-methods
    """Common class for Titanic transformation tests.
    """
    def test_transform(self, actor: task.Actor, dataset: pd.DataFrame, expected: pd.DataFrame):
        """Unit test action - ensuring the actor transformation of the input dataset returns expected values.
        """
        assert expected.equals(actor.apply(dataset))


class TestNaNImputer(Transformer):
    """Unit tests fo the statefull NaNImputer.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def dataset() -> pd.DataFrame:
        """Input dataset fixture.
        """
        return pd.DataFrame({
            'int': [0, 1, 2, None, 3, np.nan],
            'float': [0.1, 1.2, None, 2.3, np.nan, 3.4],
            'str': ['foo', None, 'foo', np.nan, 'bar', '']
        })

    @staticmethod
    @pytest.fixture(scope='function')
    def expected(dataset: pd.DataFrame) -> pd.DataFrame:
        """Expected output dataframe as a result of the transformation.
        """
        return pd.DataFrame({
            'int': dataset['int'].fillna(dataset['int'].median()),
            'float': dataset['float'].fillna(dataset['float'].median()),
            'str': dataset['str'].fillna(dataset['str'].value_counts().index[0])
        })

    @staticmethod
    @pytest.fixture(scope='function')
    def actor(dataset: pd.DataFrame) -> preprocessing.NaNImputer:
        """Actor instance under the test.
        """
        instance = preprocessing.NaNImputer().spec()
        instance.train(dataset, None)
        return instance


class TestTitleParser(Transformer):
    """Unit testing the stateless TitleParser transformer.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def source() -> str:
        """Fixture for the source column name.
        """
        return 'Name'

    @staticmethod
    @pytest.fixture(scope='function')
    def target() -> str:
        """Fixture for the target column name.
        """
        return 'Title'

    @staticmethod
    @pytest.fixture(scope='function')
    def dataset(source: str) -> pd.DataFrame:
        """Input dataset fixture.
        """
        return pd.DataFrame({
            'foo': [0, 1, 2, 3],
            source: ['Smith, Mr. John', 'Black, Ms. Jane', 'Brown, Mrs. Jo', 'White, Ian'],
        })

    @staticmethod
    @pytest.fixture(scope='function')
    def expected(dataset: pd.DataFrame, target: str) -> pd.DataFrame:
        """Expected output dataframe as a result of the transformation.
        """
        dataset[target] = ['Mr', 'Ms', 'Mrs', 'Unknown']
        return dataset

    @staticmethod
    @pytest.fixture(scope='function')
    def actor(source: str, target: str) -> preprocessing.parse_title:
        """Actor instance under the test.
        """
        return preprocessing.parse_title(source=source, target=target).spec()
