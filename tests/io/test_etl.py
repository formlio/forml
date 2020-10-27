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
ETL tests.
"""
# pylint: disable=no-self-use
import cloudpickle

import pytest

from forml import error
from forml.io import etl
from forml.io.dsl.schema import frame


class TestField:
    """Field unit tests.
    """
    def test_renamed(self, student: frame.Table):
        """Test the field renaming.
        """
        assert student.schema.dob.renamed('foo').name == 'foo'


class TestSchema:
    """Schema unit tests.
    """
    def test_serilizable(self, student: frame.Table):
        """Test schema serializability.
        """
        assert cloudpickle.loads(cloudpickle.dumps(student.schema)) == student.schema
        assert cloudpickle.loads(cloudpickle.dumps(student)) == student


class TestSource:
    """Source unit tests.
    """
    def test_query(self, student: frame.Table):
        """Test the query setup.
        """
        with pytest.raises(error.Invalid):
            etl.Source.query(student, student.score)
        query = etl.Source.query(student)
        assert isinstance(query.extract.train, frame.Query)
