"""
Dummy project source.
"""
from forml.etl.dsl.schema import kind

from forml import etl
from forml.project import component


class HelloWorld(etl.Schema):
    """Base table.
    """
    name = etl.Field(kind.String())


INSTANCE = etl.Source(HelloWorld.select(HelloWorld.name))
component.setup(INSTANCE)
