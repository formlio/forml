"""
Dummy project source.
"""
from forml import etl
from forml.etl import expression
from forml.project import component

INSTANCE = etl.Source(etl.Extract(expression.Select()))
component.setup(INSTANCE)
