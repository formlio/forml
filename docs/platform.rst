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

.. _platform:

Runtime Platform
================

ForML platform is an environment configured to allow performing particular :ref:`life cycle
actions <lifecycle>` on a general ForML :ref:`project <project>`. Thanks to the pluggable
:ref:`provider architecture <provider>`, a ForML platform can be built up using a number of
different technologies optimized for specific use cases while keeping the same interface
and thus guaranteeing the portability of the implemented projects.


Setup
-----

Make sure to :ref:`install <install>` all the necessary ForML components before proceeding to the
next sections.

.. _platform-config:

Configuration File
^^^^^^^^^^^^^^^^^^

ForML platform uses the `TOML <https://toml.io/>`_ file format for its configuration. The system
will try to locate and merge the :file:`config.toml` file instances in the following directories
(in order of parsing/merging - later overrides previous):

+-----------------+---------------------------------------------------------------------------------+
| Location        | Meaning                                                                         |
+=================+=================================================================================+
| ``/etc/forml/`` | The *system*-wide global configuration directory                                |
+-----------------+---------------------------------------------------------------------------------+
| ``~/.forml/``   | *User* home directory configuration (unless overridden by the ``$FORML_HOME`` ) |
+-----------------+---------------------------------------------------------------------------------+
| ``$FORML_HOME`` | Environment variable driven location of the *user* configuration directory      |
+-----------------+---------------------------------------------------------------------------------+

.. note::
   Both the *system* and the *user* configuration locations are also appended to the runtime
   :data:`python:sys.path` so any python modules stored in the configuration directories are
   potentially importable. This can be useful for :ref:`custom provider <provider-custom>`
   implementations.

Following is the default content of the ForML platform configuration file:

.. literalinclude:: ../forml/setup/config.toml
   :caption: config.toml (default)
   :linenos:
   :language: toml
   :start-after: # under the License.


Providers Settings
""""""""""""""""""

The majority of the configuration file deals with setting up all the different :ref:`providers
<provider>`. The file can contain multiple instances of preconfigured providers ready to be
selected for a particular execution.

The common structure for the provider configuration sections is:

.. code-block:: ini

    [<PROVIDER TYPE>]
    default = "<instance alias>"

    [<PROVIDER TYPE>.<instance alias>]
    provider = "<provider reference>"
    <provider option X> = <value X>
    ...

The meaning of the different placeholders and keywords is:

``<PROVIDER TYPE>``:
    One of the six types of provider abstractions used by ForML in *uppercase*:

    * ``REGISTRY`` - for :ref:`Model registry <registry>` providers
    * ``RUNNER`` - for :ref:`Pipeline runner <runner>` providers
    * ``FEED`` - for :ref:`Source feed <feed>` providers
    * ``SINK`` - for :ref:`Output sink <sink>` providers
    * ``INVENTORY`` - for :ref:`Application inventory <inventory>` providers
    * ``GATEWAY`` - for :ref:`Serving gateway <serving>` providers

    Each of the provider-type root sections nominates one of its instances using the ``default``
    keyword to preselect a configuration instance for situations when no explicit choice is
    specified during some particular execution.

    .. note::
       The ``FEED`` provider type can specify a list of *multiple* instances as *default*
       (contextual :ref:`feed selection <feed-selection>` is then performed at runtime).

``<instance alias>``:
    Each of the individual provider configuration instances is identified using its arbitrary
    *alias*. This alias can also be used later to explicitly choose some particular configuration
    instance when triggering an execution (i.e. using the ``-R`` :ref:`CLI <platform-cli>`
    argument).

``<provider reference>``:
    Each configuration instance must point to its :ref:`provider implementation <provider>` using
    the ``provider`` keyword. The reference can have one of two potential forms:

    * the canonical *fully qualified class name* specified as ``<full.module.path>:<class.name>`` -
      for example the :class:`forml.provider.runner.dask:Runner <forml.provider.runner.dask.Runner>`
    * the convenient *shortcut* (if defined by its implementer) - i.e. ``dask``

    .. caution::
       Shortcut references can only be used for auto-discovered provider implementations (typically
       those shipped with ForML). Any external implementations can only be referenced using the
       canonical form (plus the referred provider module must be on :data:`python:sys.path` so
       that it can be imported).

``<provider option X>``:
    Any other options specified within the provider configuration instance section are considered
    to be arbitrary arguments specific to the given provider implementation and will be passed to
    its constructor.


Logging
^^^^^^^

The :doc:`python logger <python:library/logging>` is used throughout the framework to emit
various logging messages. The :doc:`logging configuration <python:library/logging.config>` can be
customized using a :ref:`special configuration file <python:logging-config-fileformat>`
referenced in the top-level ``logcfg`` option in the main :ref:`config.toml <platform-config>`.


.. _platform-execution:

Execution Mechanisms
--------------------

ForML is using the pluggable :ref:`pipeline runners <runner>` to perform all the
possible :ref:`life cycle actions <lifecycle>`. There are three different mechanisms to carry out
the execution:

* The :ref:`command-line driven <platform-cli>` batch processing.
* Execution in the :ref:`interactive mode <interactive>` using the :class:`Virtual launcher
  <forml.runtime.Virtual>`.
* Spinning up the :ref:`serving engine <serving>` using a particular application gateway provider.


.. _platform-cli:

Command-line Interface
^^^^^^^^^^^^^^^^^^^^^^

The :ref:`life cycle <lifecycle>` management can be fully operated in batch mode using the
command-line interface - see the integrated help for more details:

.. code-block:: console

    $ forml --help
    Usage: forml [OPTIONS] COMMAND [ARGS]...

      Life Cycle Management for Data Science Projects.

    Options:
      -C, --config FILE               Additional configuration file.
      -L, --loglevel [debug|info|warning|error]
                                      Global loglevel to use.
      --logfile FILE                  Logfile path.
      --help                          Show this message and exit.

    Commands:
      application  Application command group.
      model        Model command group (production life cycle).
      project      Project command group (development life cycle).


Further details on the individual command groups can also be found in the following related
chapters:

=======================  ===============================================================
Command Group            Related Chapters
=======================  ===============================================================
``$ forml application``  :ref:`Application Management <inventory-management>`

                         :ref:`Application Publishing <application-publishing>`

                         :ref:`Serving Control <serving-gateway>`
``$ forml model``        :ref:`Model Management <registry-management>`

                         :ref:`Production Lifecycle Management <lifecycle-production>`
``$ forml project``      :ref:`Development Lifecycle Management <lifecycle-development>`
=======================  ===============================================================


Common Runtime Features
-----------------------

Core Exceptions
^^^^^^^^^^^^^^^

Following is the list of core ForML exceptions emitted at runtime:

.. autoclass:: forml.AnyError
.. autoclass:: forml.InvalidError
   :show-inheritance:
.. autoclass:: forml.MissingError
   :show-inheritance:
.. autoclass:: forml.UnexpectedError
   :show-inheritance:
.. autoclass:: forml.FailedError
   :show-inheritance:


Runtime Performance Metric
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: forml.runtime.Stats
