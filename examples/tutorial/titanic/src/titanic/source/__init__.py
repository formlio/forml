# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
