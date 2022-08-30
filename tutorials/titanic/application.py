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
Titanic application descriptor.

Using the basic ready-to-use application.Generic descriptor provides
the following features:

* loading the *latest* model generation of the project *matching*
  the application name
* attempting to decode the payload using any of the available decoders
  based on the *declared content-type*
* returning the predictions encoded using any of the available encoders
  based on the *requested content-type*
"""

from forml import application

application.setup(application.Generic('forml-example-titanic'))
