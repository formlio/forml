"""
Preprocessing unit tests.

We are thoroughly testing our preprocessing transformers.
"""
import pandas

from forml import testing
from titanic.pipeline import preprocessing


def dataframe_equals(expected: pandas.DataFrame, actual: pandas.DataFrame) -> bool:
    """DataFrames can't be simply compared for equality so we need a custom matcher.
    """
    if not actual.equals(expected):
        print(f'Dataframe mismatch: {expected} vs {actual}')
        return False
    return True


class TestNaNImputer(testing.operator(preprocessing.NaNImputer)):
    """NaNImputer unit tests.
    """
    # Dataset fixtures
    TRAINSET = pandas.DataFrame({'foo': [1., 2., 3.], 'bar': ['a', 'b', 'b']})
    TESTSET = pandas.DataFrame({'foo': [1., 4., None], 'bar': [None, 'c', 'a']})
    EXPECTED = pandas.DataFrame({'foo': [1., 4., 2.], 'bar': ['b', 'c', 'a']})

    # Test scenarios
    invalid_params = testing.Case('foo').raises(TypeError, 'takes 1 positional argument but 2 were given')
    not_trained = testing.Case().apply(TESTSET).raises(ValueError, "Must specify a fill 'value' or 'method'")
    valid_imputation = testing.Case().train(TRAINSET).apply(TESTSET).returns(EXPECTED, dataframe_equals)


class TestTitleParser(testing.operator(preprocessing.parse_title)):
    """Unit testing the stateless TitleParser transformer.
    """
    # Dataset fixtures
    INPUT = pandas.DataFrame({'Name': ['Smith, Mr. John', 'Black, Ms. Jane', 'Brown, Mrs. Jo', 'White, Ian']})
    EXPECTED = pandas.concat((INPUT, pandas.DataFrame({'Title': ['Mr', 'Ms', 'Mrs', 'Unknown']})), axis='columns')

    # Test scenarios
    invalid_params = testing.Case(foo='bar').raises(TypeError, "got an unexpected keyword argument 'foo'")
    invalid_source = testing.Case(source='Foo', target='Bar').apply(INPUT).raises(KeyError, 'Foo')
    valid_parsing = testing.Case(source='Name', target='Title').apply(INPUT).returns(EXPECTED, dataframe_equals)
