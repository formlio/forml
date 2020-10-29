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
Feed utils unit tests.
"""
# pylint: disable=no-self-use

import typing

import pytest

from forml import io
from forml.conf.parsed import provider as conf
from forml.io import sink as sinkmod


class TestHandle:
    """Sink handle unit tests.
    """
    class Conf(conf.Sink):
        """Fake override of the conf.Feed class to bypass parsing config file.
        """
        def __new__(cls, reference: str, identity: str):
            return tuple.__new__(cls, [reference, {'identity': identity}])

    @pytest.fixture(scope='session')
    def modal(self, reference: str) -> sinkmod.Handle:
        """Sink.Mode based handle fixture.
        """
        train = self.Conf(reference, 'train')
        apply = self.Conf(reference, 'apply')
        eval_ = self.Conf(reference, 'eval')
        return sinkmod.Handle(conf.Sink.Mode([train, apply, eval_]))

    @pytest.fixture(scope='session')
    def instant(self, sink: typing.Type[io.Sink]) -> sinkmod.Handle:
        """Instant based handle fixture.
        """
        return sinkmod.Handle(sink(identity='instant'))

    def test_getter(self, modal: sinkmod.Handle, instant: sinkmod.Handle):
        """Test the handle getters.
        """
        assert modal.train.identity == 'train'
        assert modal.apply.identity == 'apply'
        assert modal.eval.identity == 'eval'
        assert instant.train.identity == 'instant'
        assert instant.apply.identity == 'instant'
        assert instant.eval.identity == 'instant'
