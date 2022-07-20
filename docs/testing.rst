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

.. _testing:

Unit Testing
============

ForML provides a custom testing framework for user-defined :ref:`operators <operator>`. It
is built on top of the standard :doc:`unittest library <python:library/unittest>` with an API
specialized to cover all the standard operator outcomes while minimizing any boiler-plating.
Internally it uses the :class:`Virtual launcher <forml.runtime.Virtual>` to carry out the
particular test scenarios as a genuine ForML :ref:`workflow <workflow>` wrapping the tested
operator.

The tests need to be placed under the :file:`tests/` folder of your :ref:`project
<project-structure>` (note ``unittest`` requires all test files and the :file:`tests/` directory
itself to be python modules hence it needs to contain the appropriate
:file:`__init__.py` files).

The testing framework is available after importing the ``forml.testing`` module:

.. code-block:: python

    from forml import testing

.. seealso::
    See the :ref:`tutorials <tutorials>` for a real case unit test implementations.


Operator Test Case Outcome Assertions
-------------------------------------

The testing framework allows to assert following possible outcomes of the particular operator
under the test:

INIT_RAISES
    is a scenario, where the operator raises an *exception* right upon initialization. This can
    be used to assert an expected (hyper)parameter validation.

    Synopsis:

    .. code-block:: python

        mytest1 = testing.Case(arg1='foo').raises(ValueError, 'invalid value of arg1')

PLAINAPPLY_RAISES
    asserts an exception to be raised when executing the *apply* mode of an operator without any
    previous *train* execution.

    Synopsis:

    .. code-block:: python

        mytest2 = testing.Case(arg1='bar').apply('foo').raises(RuntimeError, 'Not trained')

PLAINAPPLY_RETURNS
    is an assertion of an output value of successful outcome of the *apply* mode executed again
    without previous *train* mode.

    Synopsis:

    .. code-block:: python

        mytest3 = testing.Case(arg1='bar').apply('baz').returns('foo')

STATETRAIN_RAISES
    checks the *train* mode of given operator fails with the expected exception.

    Synopsis:

    .. code-block:: python

        mytest4 = testing.Case(arg1='bar').train('baz').raises(ValueError, 'wrong baz')

STATETRAIN_RETURNS
    compares the output value of the successfully completed *train* mode with the expected value.

    Synopsis:

    .. code-block:: python

        mytest5 = testing.Case(arg1='bar').train('foo').returns('baz')

STATEAPPLY_RAISES
    asserts an exception to be raised from the *apply* mode when executed after previous
    successful *train* mode.

    Synopsis:

    .. code-block:: python

        mytest6 = testing.Case(arg1='bar').train('foo').apply('baz').raises(ValueError, 'wrong baz')

STATEAPPLY_RETURNS
    is a scenario, where the *apply* mode executed after previous successful *train* mode returns
    the expected value.

    Synopsis:

    .. code-block:: python

        mytest7 = testing.Case(arg1='bar').train('foo').apply('bar').returns('baz')


Operator Test Suite
-------------------
All test case assertions of the same operator are defined within the operator test suite that's
created simply as follows:

.. code-block:: python

    class TestMyTransformer(testing.operator(mymodule.MyTransformer)):
        """MyTransformer unit tests."""
        # Test scenarios
        invalid_params = testing.Case('foo').raises(TypeError, 'takes 1 positional argument but 2 were given')
        not_trained = testing.Case().apply('bar').raises(ValueError, "Must be trained ahead")
        valid_transformation = testing.Case().train('foo').apply('bar').returns('baz')

You simply create the suite by inheriting your ``Test...`` class from the ``testing.operator()``
utility wrapping your operator under the test. You then put your operator scenarios (test case
outcome assertions) right into the body of your test suite class.


Running Your Tests
------------------

All the suites are transparently expanded into full-blown :class:`python:unittest.TestCase`
definition so from here you would treat them as normal unit tests, which means you can simply run
them using the usual:

.. code-block:: console

    $ forml project test
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

All the ``.returns()`` assertions are implemented using the
:meth:`python:unittest.TestCase.assertEqual` which compares the expected and actual values
checking for :meth:`python:object.__eq__` equality. If this is not a valid comparison for the
particular data types used by the operator, you have to supply custom matcher as a second
parameter to the assertion. The matcher needs to be a callable with the following signature of
``typing.Callable[[typing.Any, typing.Any], bool]``, where the first argument is *expected* and
the second is the *actual* value.

This can be useful for example for :class:`pandas:pandas.DataFrame`, which doesn't support simple
boolean equality check. Following example uses a custom matcher for asserting the values returned
as :class:`pandas:pandas.DataFrame`:

.. code-block:: python

    def size_equals(expected: object, actual: object) -> bool:
        """Custom object comparison logic based on their size."""
        return sys.getsizeof(actual) == sys.getsizeof(expected)


    class TestFooBar(testing.operator(FooBar)):
        """Unit testing the FooBar operator."""
        # Dataset fixtures
        INPUT = ...
        EXPECTED = ...

        # Test scenarios
        valid_parsing = testing.Case().apply(INPUT).returns(EXPECTED, size_equals)


For convenience, there is a number of explicit matchers provided as part of the ``forml.testing``
package:

.. autofunction:: forml.testing.pandas_equals
