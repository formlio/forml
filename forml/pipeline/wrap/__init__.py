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
Decorators for creating operators and actors by wrapping generic (non-ForML) implementations.

Instead of creating ForML :ref:`actors <actor-decorated>` and/or :ref:`operators <operator-wrapped>`
by fully implementing their relevant base classes, they can (in special cases) be conveniently
defined using the wrappers provided within this module.
"""

from ._actor import Actor
from ._auto import AUTO, Auto, AutoSklearnClassifier, AutoSklearnRegressor, AutoSklearnTransformer, importer
from ._operator import Operator

#: The default list of :class:`auto-wrapper <forml.pipeline.wrap.Auto>` implementations
#: to be used by the :func:`wrap.importer <forml.pipeline.wrap.importer>` context manager.
AUTO = AUTO  # pylint: disable=self-assigning-variable
# hack to make AUTO visible to autodoc (otherwise ignores module attributes without docstrings)


__all__ = [
    'Actor',
    'Auto',
    'AUTO',
    'AutoSklearnTransformer',
    'AutoSklearnClassifier',
    'AutoSklearnRegressor',
    'importer',
    'Operator',
]
