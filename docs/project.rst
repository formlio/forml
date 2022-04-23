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

Project
=======

Starting New Project
--------------------

ForML project can either be created manually from scratch by defining the `component structure`_ or simply using the
``init`` subcommand of the ``forml`` :ref:`platform-cli`::

    $ forml init myproject


Component Structure
-------------------

ForML project is defined as a set of specific components wrapped into a python package with the usual
:doc:`Setuptools <setuptools>` layout. The framework offers the
*Convention over Configuration* approach for organizing the internal package structure, which means it automatically
discovers the relevant project components if the author follows this convention (there is still an option to ignore the
convention, but the author is then responsible for configuring all the otherwise automatic steps himself).

The convention is simply based on implementing specific python *modules* (or *packages*) within the project
namespace root. ForML doesn't care whether the component is defined as a module (a file with ``.py`` suffix) or
a package (a subdirectory with ``__init__.py`` file in it) since both have the same import syntax.

These naming conventions for the different project components are described in the following subsections. The general
project component structure wrapped within the python application layout might look similar to this::

    <project_name>
      ├── setup.py
      ├── <optional_project_namespace>
      │     └── <project_name>
      │          ├── __init__.py
      │          ├── pipeline  # here the component is a package
      │          │    ├── __init__.py
      │          │    ├── <moduleX>.py  # arbitrary user defined module
      │          │    └── <moduleY>.py
      │          ├── source.py
      │          ├── evaluation.py  # here the component is just a module
      │          ├── schedule.py
      │          └── tuning.py
      ├── tests
      │    ├── __init__.py
      │    ├── test_pipeline.py
      │    └── ...
      ├── README.md
      └── ...


The individual project components defined in the specific modules described below need to be hooked up into the ForML
framework using the ``project.setup()`` as shown in the examples below.

.. autofunction:: forml.project.setup

.. _project-setup:

Setup.py
''''''''

This is the standard :doc:`Setuptools <setuptools>` module with few extra features added to allow the project structure
customization and integration of the *Research lifecycle* as described in :doc:`lifecycle` sections (ie the ``eval`` or
``upload`` commands).

To hook in this extra functionality, the ``setup.py`` just needs to use the ``forml.project.Distribution`` as the
custom setupttols ``disctlass`` . The rest is the usual ``setup.py`` content::

    from forml import project

    setuptools.setup(name='forml-example-titanic',
                     version='0.1.dev0',
                     packages=setuptools.find_packages(include=['titanic*']),
                     setup_requires=['forml'],
                     install_requires=['scikit-learn', 'pandas', 'numpy', 'category_encoders==2.0.0'],
                     distclass=project.Distribution)

.. note:: The specified ``version`` value will become the *release* identifier upon *uploading* (as part of the
          *Research lifecycle*) thus needs to be a valid :pep:`440` version.

The project should carefully specify all of its dependencies using the ``install_requires`` parameter as these will be
included in the released ``.4ml`` package.

The only addition provided on top of the original ``setuptools`` functionality is the ability to customize the
conventional project component layout. If from some reason the user wants to divert from this convention, he can specify
the custom locations of its project components using the ``component`` parameter as follows::

    setuptools.setup(...,
                     component={'pipeline': 'path.to.my.custom.pipeline.module'})

.. _project-pipeline:

Pipeline Topology (``pipeline.py``)
'''''''''''''''''''''''''''''''''''

Pipeline definition is the heart of the project component structure. The framework needs to understand the
pipeline as a *Directed Acyclic Task Dependency Graph*. For this purpose, it comes with a concept of *Operators* that
the user is supplying with actual functionality (ie feature transformer, classifier) and *composing* together to
define the final flow.

The pipeline is specified in terms of the *workflow expression API* which is in detail described in the
:doc:`workflow` sections.

Same as for the other project components, the final pipeline expression defined in the ``pipeline.py`` needs to be
exposed to the framework via the ``project.setup()`` handler::

    from forml import project
    from titanic.pipeline import preprocessing, model

    FLOW = preprocessing.NaNImputer() >> model.LR(random_state=42, solver='lbfgs')
    project.setup(FLOW)


.. _project-source:

Dataset Specification (``source.py``)
'''''''''''''''''''''''''''''''''''''

This component is a fundamental part of the :doc:`IO concept<io>`. A project can define the ETL process of sourcing
data into the pipeline using the :doc:`DSL <dsl>` referring to some :ref:`catalogized schemas
<io-catalogized-schemas>` that are at runtime resolved via the available :doc:`feeds <feed>`.

The source component is provided in form of a descriptor that's created using the ``.query()`` method as shown in the
example below or documented in the :ref:`Source Descriptor Reference <io-source-descriptor>`.

.. note:: The descriptor allows to further compose with other operators using the usual ``>>`` syntax. Source
          composition domain is separate from the main pipeline so adding an operator to the source composition vs
          pipeline composition might have a different effect.

Part of the dataset specification can also be a reference to the *ordinal* column (used for determining data ranges for
splitting or incremental operations) and *label* columns for supervised learning/evaluation.

The Source descriptor again needs to be submitted to the framework using the ``project.setup()`` handler::

    from forml import project
    from forml.lib.pipeline import payload
    from openschema import kaggle as schema

    FEATURES = schema.Titanic.select(
        schema.Titanic.Pclass,
        schema.Titanic.Name,
        schema.Titanic.Sex,
        schema.Titanic.Age,
        schema.Titanic.SibSp,
        schema.Titanic.Parch,
        schema.Titanic.Ticket,
        schema.Titanic.Fare,
        schema.Titanic.Cabin,
        schema.Titanic.Embarked,
    )

    ETL = project.Source.query(FEATURES, schema.Titanic.Survived) >> payload.to_pandas([f.name for f in FEATURES.schema])
    project.setup(ETL)


.. _project-evaluation:

Evaluation Strategy (``evaluation.py``)
'''''''''''''''''''''''''''''''''''''''

Definition of the model evaluation strategy for both the development (backtesting) and production
:doc:`lifecycle <lifecycle>`.

.. note:: The whole evaluation implementation is an interim and more robust concept with different API is on the
.roadmap.

The evaluation strategy again needs to be submitted to the framework using the ``project.setup()`` handler::

    from sklearn import model_selection, metrics
    from forml import project
    from forml.lib.pipeline.evaluation import metric, method

    EVAL = project.Evaluation(
            metric.Function(metrics.log_loss),
            method.CrossVal(model_selection.StratifiedKFold(n_splits=2, shuffle=True, random_state=42)),
    )
    project.setup(EVAL)


.. _project-tuning:

Hyperparameter Tuning Strategy (``tuning.py``)
''''''''''''''''''''''''''''''''''''''''''''''


.. _project-schedule:

Scheduling Rules (``schedule.py``)
''''''''''''''''''''''''''''''''''



Tests
'''''

ForML has a rich operator unit testing facility (see the :doc:`testing` sections) which can be integrated into the usual
``tests/`` project structure.
