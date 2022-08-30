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


Deployment and Serving
======================

With all the :ref:`lifecycle actions <lifecycle>` simply :doc:`at our fingertips <lifecycle>`,
let's now focus on deploying the project as a :ref:`ForML application <application>` making it
available for :ref:`serving <serving>`.


Creating and Publishing the Application Descriptor
--------------------------------------------------

ForML applications are :ref:`implemented <application-implementation>` in form of a *descriptor*
instance defined within a *Python module*. Due to the potentially non-exclusive nature of the
:ref:`project-application relationship <application-prjrelation>`, this module might in general
need to be maintained out of the project scope but for the sake of this tutorial let's assume a
direct 1:1 relationship so that we can keep it along with the project sources in a module called
:file:`application.py`. Our top-level project structure is then going to look as follows:

.. code-block:: console

    $ ls -1p forml-tutorial-titanic
    application.py
    notebooks/
    setup.py
    tests/
    titanic/


For simplicity, we choose to :ref:`implement the application <application-implementation>` in
the most basic (yet full-featured) manner by reusing the existing :class:`application.Generic
<forml.application.Generic>` descriptor and passing its instance to :func:`application.setup()
<forml.application.setup>` for registration. Note this simplistic setup requires the application
name to match the project name (in order to select the relevant assets from the :ref:`model
registry <registry>`):

.. literalinclude:: ../../../tutorials/titanic/application.py
   :caption: application.py
   :linenos:
   :language: python
   :start-after: # under the License.

That's all it takes to implement a simple application descriptor. It can now be deployed by
the means of publishing into a :ref:`platform-configured <platform-config>` application
:ref:`inventory <inventory>`:

.. code-block:: console

    $ forml application put application.py
    $ forml application list
    forml-example-titanic


Serving
-------

Easiest way to expose our model for serving is to spin up a particular :ref:`serving gateway
<serving-gateway>` provider linked through the :ref:`platform configuration <platform-config>` to
the same :ref:`inventory <inventory>` and :ref:`registry <registry>` holding our application and
models respectively.

The configured gateway (the :class:`rest.Gateway <forml.provider.gateway.rest.Gateway>` in our case)
can be started simply using the CLI:

.. code-block:: console

    $ forml application serve
    INFO:     Started server process [568798]
    INFO:     Waiting for application startup.
    INFO:     Application startup complete.
    INFO:     Uvicorn running on http://127.0.0.1:8080 (Press CTRL+C to quit)

Note the gateway is capable of serving any application in the linked inventory.

Let's explore the capabilities using manual ``curl`` queries:

#. Querying a non-existent application ``foobarbaz``:

   .. code-block:: console

       $ curl -X POST http://127.0.0.1:8080/foobarbaz
       Application foobarbaz not found in Dispatch-registry

#. Querying our ``forml-titanic-example`` application using a *JSON* encoded payload:

   .. code-block:: console

       $ curl -X POST -H 'Content-Type: application/json' -d '[{"Pclass":1, "Name":"Foo", "Sex": "male", "Age": 34, "SibSp": 3, "Parch": 2, "Ticket": "13", "Fare": 10.1, "Cabin": "123", "Embarked": "S"}]' http://127.0.0.1:8080/forml-example-titanic
       [{"c0":0.3459976655}]

#. Making the same query but requesting the result to be encoded as CSV:

   .. code-block:: console

       $ curl -X POST -H 'Content-Type: application/json' -H 'Accept: text/csv' -d '[{"Pclass":1,"Name":"Foo", "Sex": "male", "Age": 34, "SibSp": 3, "Parch": 2, "Ticket": "13", "Fare": 10.1, "Cabin": "123", "Embarked": "S"}]' http://127.0.0.1:8080/forml-example-titanic
       c0
       0.34599766550668526

#. Making the same query but sending the payload in the *pandas-split* (JSON) format and requesting
   the result as (JSON) *values*:

   .. code-block:: console

       $ curl -X POST -H 'Content-Type: application/json; format=pandas-split' -H 'Accept: application/json; format=pandas-values' -d '{"columns": ["Pclass", "Name", "Sex", "Age", "SibSp", "Parch", "Ticket", "Fare", "Cabin", "Embarked"], "data": [[1, "Foo", "male", 34, 3, 2, 13, 10.1, "123", "S"]]}' http://127.0.0.1:8080/forml-example-titanic
       [[0.3459976655]]

That concludes this Titanic Challenge tutorial, from here you can continue to the other
:ref:`available tutorials <tutorials>` or browse the general :doc:`ForML documentation
<../../index>`.
