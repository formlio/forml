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
the forml loader. In this case it is implemented as a python module but it could be as well a package
`source/__init__.py` (to potentially split it into additional informal submodules).
"""

from openschema import kaggle as schema

from forml import project
from forml.pipeline import payload

FEATURES = schema.Titanic.select(
    schema.Titanic.Pclass,
    schema.Titanic.Name,
    schema.Titanic.Sex,
    schema.Titanic.Age,
    schema.Titanic.SibSp,
    schema.Titanic.Parch,
    schema.Titanic.Fare,
    schema.Titanic.Embarked,
)

ETL = project.Source.query(FEATURES, schema.Titanic.Survived) >> payload.to_pandas([f.name for f in FEATURES.schema])
project.setup(ETL)
