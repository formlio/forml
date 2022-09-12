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
Advanced operators for aggregating multiple models into an *ensemble*.

Model ensembling is a powerful technique for improving the overall accuracy of multiple weak
learners.

Ensembling comes in a number of different flavors each with its strengths and trade-offs. This
module provides some major implementations.
"""

from ._stacking import FullStack

__all__ = ['FullStack']
