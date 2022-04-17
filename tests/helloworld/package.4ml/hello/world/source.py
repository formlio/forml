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
Dummy project source.
"""

from forml import project
from forml.io import dsl, layout
from forml.io.dsl import function
from forml.pipeline import topology
from tests import helloworld as schema

school_ref = schema.School.reference('bar')
QUERY = (
    schema.Student.join(schema.Person, schema.Student.surname == schema.Person.surname)
    .join(school_ref, schema.Student.school == school_ref.sid)
    .select(
        schema.Student.surname,  # pylint: disable=no-member
        school_ref['name'].alias('school'),
        function.Cast(schema.Student.score, dsl.Integer()).alias('score'),
    )
    .where(schema.Student.score > 0)
    .orderby(schema.Student.updated, schema.Student['surname'])
    .limit(10)
)

OUTPUT = dsl.Schema.from_fields()


@topology.Mapper.operator
@topology.Function.actor
def as_tuple(data: layout.RowMajor) -> layout.RowMajor:
    """Tuple transformation operator."""
    return tuple(tuple(r) for r in data)


INSTANCE = (
    project.Source.query(QUERY, schema.Student.level, ordinal=schema.Student.updated)
    >> as_tuple()  # pylint: disable=no-value-for-parameter
)
project.setup(INSTANCE)
