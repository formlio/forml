"""
ETL unit tests.
"""
# pylint: disable=no-self-use

import pytest
from forml import etl

from forml.etl.dsl.schema import frame, kind
from . import Queryable


class TestSchema:
    """Table schema unit tests.
    """
    def test_empty(self):
        """Test empty schema with no fields.
        """
        with pytest.raises(TypeError):
            class Empty(etl.Schema):
                """Schema with no fields.
                """
            _ = Empty

    def test_colliding(self, student: frame.Table):
        """Test schema with colliding field names.
        """
        with pytest.raises(TypeError):
            class Colliding(student.__schema__):
                """Schema with colliding field names.
                """
                birthday = etl.Field(kind.Integer())
            _ = Colliding

    def test_access(self, student: frame.Table):
        """Test the schema access methods.
        """
        assert tuple(student.__schema__) == ('surname', 'dob', 'level', 'score', 'school')
        assert student.dob.name == 'birthday'


class TestTable(Queryable):
    """Table unit tests.
    """
    def test_fields(self, student: frame.Table):
        """Fields getter tests.
        """
        assert student.dob.name == 'birthday'
        assert student.score.name == 'score'
        with pytest.raises(AttributeError):
            _ = student.xyz

    @staticmethod
    @pytest.fixture(scope='session')
    def source(student: frame.Table) -> frame.Queryable:
        return student
