"""
Titanic data source.
"""

from forml import etl
from forml.etl import expression
from forml.project import component

from titanic.source import producer, label


TRAIN = expression.Select(producer.trainset)
PREDICT = expression.Select(producer.testset)

ETL = etl.Extract(PREDICT, TRAIN) >> label.EXTRACTOR
component.setup(ETL)
