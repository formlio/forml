"""
Titanic data source.

This is one of the main _formal_ forml components (along with `pipeline` and `evaluation`) that's being looked up by
the forml loader. In this case it is implemented as a python package but it could be as well just a module
`source.py`.

All the submodules of this packages have no semantic meaning for ForML - they are completely informal and have been
created just for structuring the project code base splitting it into these particular parts with arbitrary names.
"""
from titanic.source import producer, label

from forml.io import etl
from forml.project import component

TRAIN = etl.Select(producer.trainset)
PREDICT = etl.Select(producer.testset)

ETL = etl.Source.query(TRAIN, PREDICT) >> label.Extractor(column='Survived')
component.setup(ETL)
