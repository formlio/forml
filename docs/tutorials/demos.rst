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

.. _demos:

Pipeline Demos
==============

This chapter presents a number of ForML pipelines demonstrating the :ref:`workflow concept
<workflow>`.

To visualize the assembled workflow DAGs, we are going to execute the pipelines using the
:class:`Graphviz runner <forml.provider.runner.graphviz.Runner>`.


.. rubric:: Common Code
.. literalinclude:: ../../tutorials/demos/__init__.py
  :caption: tutorials/demos/__init__.py
  :linenos:
  :language: python
  :start-at: import

Mini
----

* :ref:`system nodes <topology-compiler>`
* :ref:`feed <feed>` - including label extraction
* :ref:`sink <sink>` (captor)
* :ref:`operator auto-wrapping <operator-autowrap>`

.. literalinclude:: ../../tutorials/demos/d1_mini.py
  :caption: tutorials/demos/d1_mini.py
  :linenos:
  :language: python
  :start-at: import
  :end-at: LAUNCHER

.. md-tab-set::

    .. md-tab-item:: Train Mode

        .. code-block:: python

            LAUNCHER.train()

        .. image:: ../_static/images/demos-mini-train.png
          :target: ../_static/images/demos-mini-train.png

    .. md-tab-item:: Apply Mode

        .. code-block:: python

            LAUNCHER.apply()

        .. image:: ../_static/images/demos-mini-apply.png
          :target: ../_static/images/demos-mini-apply.png

Simple
------

* :ref:`operator composition <operator-composition>`

.. literalinclude:: ../../tutorials/demos/d2_simple.py
  :caption: tutorials/demos/d2_ensemble.py
  :linenos:
  :language: python
  :start-at: import
  :end-at: LAUNCHER

.. md-tab-set::

    .. md-tab-item:: Train Mode

        .. code-block:: python

            LAUNCHER.train()

        .. image:: ../_static/images/demos-simple-train.png
          :target: ../_static/images/demos-simple-train.png

    .. md-tab-item:: Apply Mode

        .. code-block:: python

            LAUNCHER.apply()

        .. image:: ../_static/images/demos-simple-apply.png
          :target: ../_static/images/demos-simple-apply.png


Ensemble
--------

* :mod:`forml.pipeline.ensemble`

.. literalinclude:: ../../tutorials/demos/d3_ensemble.py
  :caption: tutorials/demos/d3_ensemble.py
  :linenos:
  :language: python
  :start-at: import
  :end-at: LAUNCHER

.. md-tab-set::

    .. md-tab-item:: Train Mode

        .. code-block:: python

            LAUNCHER.train()

        .. image:: ../_static/images/demos-ensemble-train.png
          :target: ../_static/images/demos-ensemble-train.png

    .. md-tab-item:: Apply Mode

        .. code-block:: python

            LAUNCHER.apply()

        .. image:: ../_static/images/demos-ensemble-apply.png
          :target: ../_static/images/demos-ensemble-apply.png


Complex
-------

.. literalinclude:: ../../tutorials/demos/d4_complex.py
  :caption: tutorials/demos/d4_complex.py
  :linenos:
  :language: python
  :start-at: import
  :end-at: LAUNCHER

.. md-tab-set::

    .. md-tab-item:: Train Mode

        .. code-block:: python

            LAUNCHER.train()

        .. image:: ../_static/images/demos-complex-train.png
          :target: ../_static/images/demos-complex-train.png

    .. md-tab-item:: Apply Mode

        .. code-block:: python

            LAUNCHER.apply()

        .. image:: ../_static/images/demos-complex-apply.png
          :target: ../_static/images/demos-complex-apply.png


Custom
------

* :ref:`wrapped actors <actor-decorated>`
* :ref:`wrapped operators <operator-wrapped>`

.. literalinclude:: ../../tutorials/demos/d5_custom.py
  :caption: tutorials/demos/d5_custom.py
  :linenos:
  :language: python
  :start-at: import
  :end-at: LAUNCHER

.. md-tab-set::

    .. md-tab-item:: Train Mode

        .. code-block:: python

            LAUNCHER.train()

        .. image:: ../_static/images/demos-custom-train.png
          :target: ../_static/images/demos-custom-train.png

    .. md-tab-item:: Apply Mode

        .. code-block:: python

            LAUNCHER.apply()

        .. image:: ../_static/images/demos-custom-apply.png
          :target: ../_static/images/demos-custom-apply.png
