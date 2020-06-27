"""
Dummy project source.
"""
from forml.io.dsl.schema import kind

from forml.io import etl
from forml.project import component


class HelloWorld(etl.Schema):
    """Base table.
    """
    name = etl.Field(kind.String())


INSTANCE = etl.Source.query(HelloWorld.select(HelloWorld.name))
component.setup(INSTANCE)
