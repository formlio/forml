"""
Global ForML unit tests fixtures.
"""
# pylint: disable=no-self-use

import pytest

from forml import etl
from forml.etl.schema import kind, frame


@pytest.fixture(scope='session')
def person() -> frame.Table:
    """Base table fixture.
    """

    class Person(etl.Schema):
        """Base table.
        """
        surname = etl.Field(kind.String())
        dob = etl.Field(kind.Date(), 'birthday')

    return Person


@pytest.fixture(scope='session')
def student(person: frame.Table) -> frame.Table:
    """Extended table fixture.
    """

    class Student(person):
        """Extended table.
        """
        level = etl.Field(kind.Integer())
        score = etl.Field(kind.Float())
        school = etl.Field(kind.Integer())

    return Student


@pytest.fixture(scope='session')
def school() -> frame.Table:
    """School table fixture.
    """

    class School(etl.Schema):
        """School table.
        """
        sid = etl.Field(kind.Integer(), 'id')
        name = etl.Field(kind.String())

    return School
