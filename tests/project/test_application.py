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
Application descriptor tests.
"""
# pylint: disable=no-self-use
import pathlib
import pickle

import pytest

import forml
from forml import project


class TestDescriptor:
    """Application descriptor unit tests."""

    def test_instantiable(self, descriptor: type[project.Descriptor]):
        """Test the descriptor can not be instantiated."""
        with pytest.raises(TypeError, match='Descriptor not instantiable'):
            descriptor()

    def test_application(self, descriptor: type[project.Descriptor], application: str):
        """Test the retrieval of the descriptor application name."""
        assert descriptor.application == application

    def test_serializable(self, descriptor: type[project.Descriptor]):
        """Descriptor serializability test."""
        assert pickle.loads(pickle.dumps(descriptor)) == descriptor


class TestDescriptorHandle:
    """Descriptor handle tests."""

    def test_invalid(self, project_path: pathlib.Path):
        """Test invalid handle setup."""
        with pytest.raises(forml.InvalidError, match='file expected'):
            project.Descriptor.Handle(project_path)  # not a
