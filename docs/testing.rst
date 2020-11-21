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

Operator Unit Testing
=====================

ForML provides a custom testing framework for user-defined operators. It is built on top of the standard ``unittest``
library with an API specialized to cover all the standard operator outcomes while minimizing any boiler-plating.

The tests need to be placed under the ``tests/`` folder of your :doc:`project` (note ``unittest`` requires all test
files and the ``tests/`` directory itself to be python modules hence it needs to contain the appropriate
``__init__.py`` files).

The testing framework is available after importing the ``forml.testing`` module::

    from forml import testing

See the :doc:`tutorial` for a real case unit test implementations.


Operator Test Case Outcome Assertions
-------------------------------------

The testing framework allows to assert following possible outcomes of the particular operator under the test:

INIT_RAISES
    is a scenario, where the operator raises an *exception* right upon initialization. This can be used to assert an
    expected (hyper)parameter validation.

    Synopsis::

        mytest1 = testing.Case(arg1='foo').raises(ValueError, 'invalid value of arg1')

PLAINAPPLY_RAISES
    asserts an exception to be raised when executing the *apply* mode of an operator without any previous *train*
    execution.

    Synopsis::

        mytest2 = testing.Case(arg1='bar').apply('foo').raises(RuntimeError, 'Not trained')

PLAINAPPLY_RETURNS
    is an assertion of an output value of successful outcome of the *apply* mode executed again without previous
    *train* mode.

    Synopsis::

        mytest3 = testing.Case(arg1='bar').apply('baz').returns('foo')

STATETRAIN_RAISES
    checks the *train* mode of given operator fails with the expected exception.

    Synopsis::

        mytest4 = testing.Case(arg1='bar').train('baz').raises(ValueError, 'wrong baz')

STATETRAIN_RETURNS
    compares the output value of the successfully completed *train* mode with the expected value.

    Synopsis::

        mytest5 = testing.Case(arg1='bar').train('foo').returns('baz')

STATEAPPLY_RAISES
    asserts an exception to be raised from the *apply* mode when executed after previous successful *train* mode.

    Synopsis::

        mytest6 = testing.Case(arg1='bar').train('foo').apply('baz').raises(ValueError, 'wrong baz')

STATEAPPLY_RETURNS
    is a scenario, where the *apply* mode executed after previous successful *train* mode returns the expected value.

    Synopsis::

        mytest7 = testing.Case(arg1='bar').train('foo').apply('bar').returns('baz')


Operator Test Suite
-------------------
All test case assertions of the same operator are defined within the operator test suite that's created simply as
follows::

    class TestMyTransformer(testing.operator(mymodule.MyTransformer)):
        """MyTransformer unit tests."""
        # Test scenarios
        invalid_params = testing.Case('foo').raises(TypeError, 'takes 1 positional argument but 2 were given')
        not_trained = testing.Case().apply('bar').raises(ValueError, "Must be trained ahead")
        valid_transformation = testing.Case().train('foo').apply('bar').returns('baz')

You simply create the suite by inheriting your ``Test...`` class from the ``testing.operator()`` utility wrapping your
operator under the test. You then put your operator scenarios (test case outcome assertions) right into the body of your
test suite class.


Running Your Tests
------------------

All the suites are transparently expanded into full-blown ``unittest.TestCase`` definition so from here you would treat
them as normal unit tests, which means you can simply run them using the usual::

    $ python3 setup.py test
    running test
    TestNaNImputer
    Test of Invalid Params ... ok
    TestNaNImputer
    Test of Not Trained ... ok
    TestNaNImputer
    Test of Valid Imputation ... ok
    TestTitleParser
    Test of Invalid Params ... ok
    TestTitleParser
    Test of Invalid Source ... ok
    TestTitleParser
    Test of Valid Parsing ... ok
    ----------------------------------------------------------------------
    Ran 6 tests in 0.591s

    OK


Custom Value Matchers
---------------------

All the ``.returns()`` assertions are implemented using the ``unittest.TestCase.assertEquals()`` which compares the
expected and actual values checking for ``__eq__()`` equality. If this is not a valid comparison for the particular
data types used by the operator, you have to supply custom matcher as a second parameter to the assertion.

This can be useful for example for ``pandas.DataFrames``, which don't support simple boolean equality check. Following
example uses a custom matcher for asserting the values returned as ``pandas.DataFrames``::


    def dataframe_equals(expected: pandas.DataFrame, actual: pandas.DataFrame) -> bool:
        """DataFrames can't be simply compared for equality so we need a custom matcher."""
        if not actual.equals(expected):
            print(f'Dataframe mismatch: {expected} vs {actual}')
            return False
        return True


    class TestTitleParser(testing.operator(preprocessing.parse_title)):
        """Unit testing the stateless TitleParser transformer."""
        # Dataset fixtures
        INPUT = pandas.DataFrame({'Name': ['Smith, Mr. John', 'Black, Ms. Jane', 'Brown, Mrs. Jo', 'White, Ian']})
        EXPECTED = pandas.concat((INPUT, pandas.DataFrame({'Title': ['Mr', 'Ms', 'Mrs', 'Unknown']})), axis='columns')

        # Test scenarios
        invalid_params = testing.Case(foo='bar').raises(TypeError, "got an unexpected keyword argument 'foo'")
        invalid_source = testing.Case(source='Foo', target='Bar').apply(INPUT).raises(KeyError, 'Foo')
        valid_parsing = testing.Case(source='Name', target='Title').apply(INPUT).returns(EXPECTED, dataframe_equals)
