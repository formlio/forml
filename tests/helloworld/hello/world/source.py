"""
Dummy project source.
"""
from forml import etl
from forml.project import component

INSTANCE = etl.Source(etl.Extract(etl.Select()))
component.setup(INSTANCE)
