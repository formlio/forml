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

.. _project:

Project Organization
====================

Projects built on ForML are in principle software source-code collections consisting of a set of
defined components organized as a python package. Their ultimate purpose is to enable effective
development leading to delivering (i.e. *releasing* a version of) a solution in form of a deployable
:ref:`artifact <registry-artifacts>`.

While developing, ForML allows execution of the project source-code working copy by triggering its
:ref:`development life cycle actions <lifecycle-development>` or when visited in the
:ref:`interactive mode <interactive>`.

.. attention::
   Although not in the scope of this documentation, all the general source-code management best
   practices (version control, continuous integration/delivery, etc.) are applicable to ForML
   projects and should be integrated into the development process.

To discover the structure of some real ForML projects, it is worth exploring the available
:ref:`tutorials <tutorials>`.

.. _project-init:

Starting a New Project
----------------------

ForML project can be initialized either manually by implementing the `component structure`_  from
scratch or simply via the ``init`` subcommand of the ``forml`` :ref:`command-line interface
<platform-cli>`:

.. code-block:: console

    $ forml project init myproject

.. _project-structure:

Component Structure
-------------------

ForML projects are organized as usual python projects accompanied with a :pep:`621` compliant
:file:`pyproject.toml`. They are structured in a way to allow ForML identifying its *principal
components* and to operate its :ref:`life cycle <lifecycle>`.

The framework adopts the `Convention over Configuration
<https://en.wikipedia.org/wiki/Convention_over_configuration>`_ approach for
organizing the internal project structure to automatically discover the relevant
components (it is still possible to ignore the convention and organize the project in an
arbitrary way, but the author is then responsible for explicitly configuring all the otherwise
automatic steps himself).

The typical project structure matching the ForML convention might look as the following tree::

    <project_name>
      ├── pyproject.toml
      ├── <optional_project_namespace_package>
      │     └── <project_root_package>
      │          ├── __init__.py
      │          ├── pipeline  # principal component as a package
      │          │    ├── __init__.py
      │          │    └── <moduleX>.py  # arbitrary module not part of the convention
      │          ├── source.py  # principal component as a module
      │          ├── evaluation.py
      │          ├── <moduleY>.py  # another module not part of the convention
      │          └── tuning.py
      ├── tests
      │    ├── __init__.py
      │    ├── test_<pipeline>.py  # actual name not part of the convention
      │    └── ...
      ├── README.md  # not part of the convention
      ├── notebooks  # not part of the convention
      │    └── ...
      └── ...

Clearly, the overall structure does not look any special - pretty usual python project layout
(plus some additional content). What makes it a ForML project is the particular modules and/or
packages within that structure and specific metadata provided in the :file:`pyproject.toml`. Let's
focus on each of these components in the following sections.


.. _project-descriptor:

Project Descriptor
^^^^^^^^^^^^^^^^^^

This is a standard :pep:`pyproject.toml <621>` metadata descriptor with a specific ForML ``tool``
section helping to integrate the ForML principal component structure. It's placed directly in the
project root directory.

The minimal content looks as follows:

.. code-block:: toml

    [project]
    name = "forml-tutorial-titanic"
    version = "0.1.dev1"
    dependencies = [
        "openschema",
        "scikit-learn",
        "pandas",
        "numpy",
    ]

    [tool.forml]
    package = "titanic"

The ``[project]`` section can contain any additional metadata supported by the :pep:`621`
specification.

.. note::
    Upon publishing (in the scope of the :ref:`development life cycle <lifecycle-development>`), the
    specified ``[project.version]`` value will become the *release* identifier and thus needs to
    be a valid :pep:`440` version.

The project should carefully specify all of its dependencies using the ``[project.dependencies]``
list as these will be included in the released :ref:`.4ml package artifact <registry-artifacts>`.

The custom ``[tool.forml]`` section supports the following options:

* the ``package`` string referring to the python package containing the principal components
* the optional ``components`` map allowing to override the conventional modules representing the
  individual principal components as submodules relatively to the ``package``:

  .. code-block:: toml

      [tool.forml.components]
      evaluation = "relative.path.to.my.custom.evaluation.module"
      pipeline = "relative.path.to.my.custom.pipeline.module"
      source = "relative.path.to.my.custom.source.module"


.. _project-principal:

Principal Components
^^^^^^^^^^^^^^^^^^^^

These are the actual high-level blocks of the particular ForML solution provided as python
*modules* (or *packages*) within the project package root.

.. hint::
    ForML does not care whether the principal component is defined as a module (a file with ``.py``
    suffix) or a package (a subdirectory with :file:`__init__.py` file in it) since both have the
    same import syntax.

To load each of the principal components, ForML relies on the ``project.setup()`` function as the
expected component registration interface:

.. autofunction:: forml.project.setup


.. _project-pipeline:

Pipeline Expression
"""""""""""""""""""

Pipeline definition is the heart of the entire solution. It is provided in form of the
:ref:`workflow expression <workflow-expression>`.

ForML expects this component to be provided as a :file:`pipeline.py` module or :file:`pipeline`
package under the project package root.

.. code-block:: python
   :caption: pipeline.py or pipeline/__init__.py
   :linenos:

    from forml import project
    from . import preprocessing, model  # project-specific implementations

    PIPELINE = preprocessing.Imputer() >> model.Classifier(random_state=42)
    project.setup(PIPELINE)


.. _project-source:

Dataset Definition
""""""""""""""""""

The ``source`` component provides the project with a definite while still portable dataset
description. It is specified using the :meth:`project.Source.query <forml.project.Source.query>`
as a :ref:`DSL expression <query>` against some particular :ref:`schema catalog <io-catalog>`.

.. code-block:: python
   :caption: source.py or source/__init__.py
   :linenos:

    from forml import project
    from forml.pipeline import payload
    from openschema import kaggle as schema

    FEATURES = schema.Titanic.select(
        schema.Titanic.Pclass,
        schema.Titanic.Name,
        schema.Titanic.Sex,
        schema.Titanic.Age,
        schema.Titanic.SibSp,
        schema.Titanic.Parch,
        schema.Titanic.Fare,
        schema.Titanic.Embarked,
    ).orderby(schema.Titanic.PassengerId)

    SOURCE = (
        project.Source.query(FEATURES, schema.Titanic.Survived)
        >> payload.ToPandas(columns=[f.name for f in FEATURES.schema])
    )
    project.setup(SOURCE)


.. _project-evaluation:

Evaluation Strategy
"""""""""""""""""""

Definition of the :ref:`model evaluation strategy <evaluation>` for both the development and
production :ref:`life cycles <lifecycle>` provided as the
:class:`project.Evaluation <forml.project.Evaluation>` descriptor.

.. code-block:: python
   :caption: evaluation.py or evaluation/__init__.py
   :linenos:

    from sklearn import metrics
    from forml import evaluation, project

    EVALUATION = project.Evaluation(
        evaluation.Function(metrics.log_loss),
        evaluation.HoldOut(test_size=0.2, stratify=True, random_state=42),
    )
    project.setup(EVALUATION)


Tests
^^^^^

ForML has a rich operator unit testing facility that can be integrated into the usual
:file:`tests/` project structure. This topic is extensively covered in the separate :ref:`testing`
chapter.
