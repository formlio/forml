"""
Global ForML unit tests fixtures.
"""
# pylint: disable=no-self-use

import pytest

from forml import etl
from forml.etl import schema, kind


@pytest.fixture(scope='session')
def person() -> schema.Table:
    """Base table fixture.
    """

    class Person(etl.Schema):
        """Base table.
        """
        surname = etl.Field(kind.String())
        dob = etl.Field(kind.Date(), 'birthday')

    return Person


@pytest.fixture(scope='session')
def student(person: schema.Table) -> schema.Table:
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
def school() -> schema.Table:
    """School table fixture.
    """

    class School(etl.Schema):
        """School table.
        """
        id = etl.Field(kind.Integer())
        name = etl.Field(kind.String())

    return School
