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
Customized DSL errors.
"""
import forml


class UnprovisionedError(forml.MissingError):
    """Source or Feature resolving exception.

    Raised by DSL parsers when the given *source* or *feature* (typically :class:`dsl.Table
    <forml.io.dsl.Table>` or :class:`dsl.Column <forml.io.dsl.Column>`) can't be resolved
    using the available data sources.
    """


class UnsupportedError(forml.MissingError):
    """Indicating DSL operation unsupported by the given parser."""


class GrammarError(forml.InvalidError):
    """Indicating syntactical error in the given DSL query statement."""
