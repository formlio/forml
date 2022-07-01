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

Runtime Platform
================

ForML platform is an environment configured to allow performing particular :doc:`lifecycle
actions <lifecycle>` on a general ForML :doc:`project <project>`. Thanks to the plugable
:doc:`provider architecture <provider>`, a ForML platform can be built up using a number of
different technologies optimized for specific use-cases while keeping the same interface
and thus guaranteeing portability of the implemented projects.


Setup
-----

Make sure to :doc:`install<install>` all the necessary ForML components before proceeding to the
next sections.

.. _platform-config:

Configuration File
^^^^^^^^^^^^^^^^^^

ForML platform uses the `TOML <https://github.com/toml-lang/toml>`_ file format for its
configuration. The system will try to locate and merge the :file:`config.toml` file instances in
the following directories (in order of parsing/merging - later overrides previous):

+-----------------+----------------------------------------------------------------------------+
| Location        | Meaning                                                                    |
+=================+============================================================================+
| ``/etc/forml/`` | *System*-wide global configuration directory                               |
+-----------------+----------------------------------------------------------------------------+
| ``~/.forml/``   | *User* homedir configuration (unless overridden by ``$FORML_HOME`` )       |
+-----------------+----------------------------------------------------------------------------+
| ``$FORML_HOME`` | Environment variable driven location of the *user* configuration directory |
+-----------------+----------------------------------------------------------------------------+

.. note::
   Both the *system* and the *user* config locations are also appended to the runtime
   :data:`python:sys.path` so any python modules stored into the config directories are potentially
   importable. This can be useful for custom :doc:`feed provider <feed>` implementations.

Following is the default content of the ForML platform configuration file:

.. literalinclude:: ../forml/conf/config.toml
   :caption: config.toml (default)
   :linenos:
   :language: toml
   :start-after: # under the License.


The majority of the configuration file deals with setting up all the different *providers*. This is
covered in great detail in the standalone chapter dedicated to the :doc:`providers
architecture <provider>` specifically.


Logging
^^^^^^^

The :doc:`python logger <python:library/logging>` is used throughout the framework to emit
various logging messages. The :doc:`logging config <python:library/logging.config>` can be
customized using a :ref:`special config file <python:logging-config-fileformat>` referenced in the
top-level ``logcfg`` option in the main :ref:`config.toml <platform-config>`.


.. _platform-mechanism:

Execution Mechanisms
--------------------

ForML is using the pluggable :doc:`pipeline runners <runner>` to perform all the
possible :doc:`lifecycle actions <lifecycle>`. There are three different mechanisms to carry out
the execution:

* The :ref:`command-line driven <platform-cli>` batch processing.
* Execution in the :doc:`interactive mode <interactive>` using the :class:`Virtual launcher
  <forml.runtime.Virtual>`.
* Spinning up the :doc:`serving engine <serving>` using a particular application gateway provider.


.. _platform-cli:

Command-line Interface
^^^^^^^^^^^^^^^^^^^^^^

The :doc:`lifecycle <lifecycle>` management can be fully operated in batch mode using the
command-line interface - see the integrated help for more details:

.. code-block:: console

    $ forml --help
    Usage: forml [OPTIONS] COMMAND [ARGS]...

      Lifecycle Management for Datascience Projects.

    Options:
      -C, --config PATH               Additional config file.
      -L, --loglevel [debug|info|warning|error]
                                      Global loglevel to use.
      --help                          Show this message and exit.

    Commands:
      application  Application command group.
      model        Model command group (production lifecycle).
      project      Project command group (development lifecycle).
