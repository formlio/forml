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
import tempfile

import pytest

import forml
from forml import project


class TestDescriptor:
    """Application descriptor unit tests."""

    def test_application(self, descriptor: project.Descriptor, application: str):
        """Test the retrieval of the descriptor application name."""
        assert descriptor.name == application

    def test_serializable(self, descriptor: project.Descriptor):
        """Descriptor serializability test."""
        assert pickle.loads(pickle.dumps(descriptor)) == descriptor


class TestDescriptorHandle:
    """Descriptor handle tests."""

    def test_invalid(self, tmp_path: pathlib.Path):
        """Test invalid handle setup."""
        with pytest.raises(forml.InvalidError, match='file expected'):
            project.Descriptor.Handle(tmp_path)  # not a file
        with tempfile.NamedTemporaryFile(dir=tmp_path, suffix='.foo') as path, pytest.raises(
            forml.InvalidError, match='not a module'
        ):
            project.Descriptor.Handle(path.name)  # not a python module
        with tempfile.NamedTemporaryFile(dir=tmp_path, suffix='.py') as path, pytest.raises(
            forml.InvalidError, match='no setup'
        ):
            project.Descriptor.Handle(path.name)  # not calling component.setup
