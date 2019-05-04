"""
Titanic preprocessing unit tests.
"""
# pylintq: disable=no-self-use, protected-access
import numpy
import pandas
import pytest
from forml.flow import task

from titanic.pipeline import preprocessing


class Transformer:
    """Common class for Titanic transformation tests.
    """
    def test_transform(self, actor: task.Actor, dataset: pandas.DataFrame, expected: pandas.DataFrame):
        """Unit test action - ensuring the actor transformation of the input dataset returns expected values.
        """
        assert expected.equals(actor.apply(dataset))


class TestNaNImputer(Transformer):
    """Unit tests fo the statefull NaNImputer.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def dataset() -> pandas.DataFrame:
        """Input dataset fixture.
        """
        return pandas.DataFrame({
            'int': [0, 1, 2, None, 3, numpy.nan],
            'float': [0.1, 1.2, None, 2.3, numpy.nan, 3.4],
            'str': ['foo', None, 'foo', numpy.nan, 'bar', '']
        })

    @staticmethod
    @pytest.fixture(scope='function')
    def expected(dataset: pandas.DataFrame) -> pandas.DataFrame:
        """Expected output dataframe as a result of the transformation.
        """
        return pandas.DataFrame({
            'int': dataset['int'].fillna(dataset['int'].median()),
            'float': dataset['float'].fillna(dataset['float'].median()),
            'str': dataset['str'].fillna(dataset['str'].value_counts().index[0])
        })

    @staticmethod
    @pytest.fixture(scope='function')
    def actor(dataset: pandas.DataFrame) -> preprocessing.NaNImputer:
        """Actor instance under the test.
        """
        instance = preprocessing.NaNImputer()._spec()
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
    def dataset(source: str) -> pandas.DataFrame:
        """Input dataset fixture.
        """
        return pandas.DataFrame({
            'foo': [0, 1, 2, 3],
            source: ['Smith, Mr. John', 'Black, Ms. Jane', 'Brown, Mrs. Jo', 'White, Ian'],
        })

    @staticmethod
    @pytest.fixture(scope='function')
    def expected(dataset: pandas.DataFrame, target: str) -> pandas.DataFrame:
        """Expected output dataframe as a result of the transformation.
        """
        dataset[target] = ['Mr', 'Ms', 'Mrs', 'Unknown']
        return dataset

    @staticmethod
    @pytest.fixture(scope='function')
    def actor(source: str, target: str) -> preprocessing.TitleParser:
        """Actor instance under the test.
        """
        return preprocessing.TitleParser(source=source, target=target)._spec()
