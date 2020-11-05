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
Kaggle's Titanic dataset schema.
"""
from forml.io.dsl import struct
from forml.io.dsl.struct import kind


class Passenger(struct.Schema):
    """Titanic: Machine Learning from Disaster.

    Variable Notes:
        pclass: A proxy for socio-economic status (SES)
            * 1st = Upper
            * 2nd = Middle
            * 3rd = Lower

        age: Age is fractional if less than 1. If the age is estimated, is it in the form of xx.5

        sibsp: The dataset defines family relations in this way...
            * Sibling = brother, sister, stepbrother, stepsister
            * Spouse = husband, wife (mistresses and fiancés were ignored)

        parch: The dataset defines family relations in this way...
            * Parent = mother, father
            * Child = daughter, son, stepdaughter, stepson

            Some children travelled only with a nanny, therefore parch=0 for them.
    """
    PassengerId = struct.Field(kind.Integer())  # Passenger ID
    Survived = struct.Field(kind.Integer())  # Survival (0 = No, 1 = Yes)
    Pclass = struct.Field(kind.Integer())  # Ticket class (1 = 1st, 2 = 2nd, 3 = 3rd)
    Name = struct.Field(kind.String())  # Passenger name
    Sex = struct.Field(kind.String())  # Sex
    Age = struct.Field(kind.Integer())  # Age in years
    SibSp = struct.Field(kind.Integer())  # # of siblings / spouses aboard the Titanic
    Parch = struct.Field(kind.Integer())  # # of parents / children aboard the Titanic
    Ticket = struct.Field(kind.Integer())  # Ticket number
    Fare = struct.Field(kind.Float())  # Passenger fare
    Cabin = struct.Field(kind.String())  # Cabin number
    Embarked = struct.Field(kind.String())  # Port of Embarkation (C = Cherbourg, Q = Queenstown, S = Southampton)
