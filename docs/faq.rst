 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
 ..   http://www.apache.org/licenses/LICENSE-2.0
 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

FAQs
====

What data format is used in the pipeline between the actors?
------------------------------------------------------------

ForML actually doesn't care. It is only responsible for wiring up the actors in the desired graph but is fairly
agnostic about the actual payload exchanged between them. It is the responsibility of the project implementor to engage
actors that understand each other.

For convenience, the :doc:`pipeline` shipped with ForML contains certain actors/operators implementations that expect
the data to be `Pandas <https://pandas.pydata.org/>`_ dataframes. This is however rather a practical choice of the flow
library (or a controversy that might get it removed from the ForML framework long term) while the ForML core is truly
independent of the data formats being passed through.


Can a Feed engage multiple reader types so that I can mix for example file based datasources with data in a DB?
---------------------------------------------------------------------------------------------------------------

No. It sounds like a cool idea to have a DSL interpreter that can just get raw data from any possible reader type and
natively implement the ETL operations on top of it, but since there are existing dedicated ETL platforms doing exactly
that (like the `Trino DB <https://trino.io/>`_, which ForML already can integrate with), trying to support the same
feature on the feed level would be unnecessarily stretching the project goals too far.
