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
IO sink utils.
"""

import typing

from forml import io
from forml.conf import provider as conf


class Handle:
    """Sink handle is a lazy wrapper around alternative sink specifiers providing a particular Sink instance upon
    request.
    """
    class Mode:
        """Handle mode getter descriptor.
        """
        def __init__(self, target: str):
            self._target: str = target

        def __get__(self, handle: 'Handle', _):
            if isinstance(handle._sink, io.Sink):  # already Sink instance
                return handle._sink
            assert isinstance(handle._sink, conf.Sink.Mode)
            descriptor: conf.Sink = getattr(handle._sink, self._target)
            return io.Sink[descriptor.reference](**descriptor.params)

    def __init__(self, sink: typing.Union[conf.Sink.Mode, str, 'io.Sink']):
        if isinstance(sink, str):
            sink = conf.Sink.Mode.resolve(sink)
        self._sink: typing.Union[conf.Sink.Mode, io.Sink] = sink

    train = Mode('train')
    apply = Mode('apply')
    eval = Mode('eval')
