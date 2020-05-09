"""
Dummy project source.
"""
from forml import etl
from forml.etl import function
from forml.project import component

INSTANCE = etl.Source(etl.Extract(function.Select()))
component.setup(INSTANCE)
