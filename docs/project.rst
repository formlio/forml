Project
=======

Starting New Project
--------------------
ForML project can either be created manually from scratch by defining the `component structure`_ or simply using the
``init`` subcommand of the ``forml`` CLI::

    $ forml init myproject


Component Structure
-------------------

ForML project is defined as a set of specific components wrapped into a python package with the usual ``setuptools``
layout. The framework offers the *Convention over Configuration* approach for organizing the internal package structure,
which means it automatically discovers the relevant project components if the author follows this convention (there is
still an option to ignore the convention, but the author is then responsible for configuring all the otherwise
automatic steps himself).

The convention is simply based on implementing specific python *modules* (or *packages*) within the project
namespace root. ForML doesn't care whether the component is defined as a module (a file with ``.py`` suffix) or
a package (a subdirectory with ``__init__.py`` file in it) since both have the same import syntax.

These naming conventions for the different project components are described in the following subsections. The general
project component structure wrapped within the python application layout might look similar to this::

    myprj
      |- setup.py
      |- src
      |    \- <project_namespace>
      |          \- myprj
      |               |- __init__.py
      |               |- pipeline  # here the component is a package
      |               |    |- __init__.py
      |               |    |- <moduleX>.py  # arbitrary user defined module
      |               |    \- <moduleY>.py
      |               |- source.py
      |               |- evaluation.py  # here the component is just a module
      |- tests
      |    |- __init__.py
      |    |- test_pipeline.py
      |    \- ...
      |- README.md
      \- ...


Setup.py
''''''''

This is the standard `setuptools <https://setuptools.readthedocs.io/en/latest/setuptools.html>`_ module with few extra
features added to allow the project structure customization and integration of the *Research lifecycle* as described in
:doc:`lifecycle` sections (ie the ``score`` or ``upload`` commands).

To hook in this extra functionality, the ``setup.py`` just needs to import ``forml.project.setuptools`` instead of the
original ``setuptools``. The rest is the usual ``setup.py`` content::

    from forml.project import setuptools

    setuptools.setup(name='forml-example-titanic',
                     version='0.1.dev0',
                     package_dir={'': 'src'},
                     packages=setuptools.find_packages(where='src'),
                     install_requires=['scikit-learn', 'pandas', 'numpy', 'category_encoders==2.0.0'])

Note the specified ``version`` value will become the *lineage* identifier upon *uploading* (as part of the *Research
lifecycle*) thus needs to be a valid `PEP 440 <https://www.python.org/dev/peps/pep-0440/>`_ version.

The project should carefully specify all of its dependencies using the ``install_requires`` parameter as these will be
included in the released ``.4ml`` package.

The only addition provided on top of the original ``setuptools`` functionality is the ability to customize the
conventional project component layout. If from some reason the user wants to divert from this convention, he can specify
the custom locations of its project components using the ``component`` parameter as follows::

    setuptools.setup(...,
                     component={'pipeline': 'path.to.my.custom.pipeline.module'})


Pipeline (``pipeline.py``)
''''''''''''''''''''''''''

Pipeline definition is the heart of the project component structure. The framework needs to understand the
pipeline as a *Directed Acyclic Task Dependency Graph*. For this purpose it comes with a concept of *Operators* that
the user is supplying with actual functionality (ie feature transformer, classifier) and *composing* together to
define the final flow.

The pipeline is specified in terms of the *workflow expression interface* which is in detail described in the
:doc:`workflow` sections.

Same as for the other project components, the final pipeline expression defined in the ``pipeline.py`` needs to be
exposed to the framework via the ``forml.project.component.setup()`` handler::

    from forml.project import component
    from titanic.pipeline import preprocessing, model
    INSTANCE = preprocessing.NaNImputer() >> model.LR(random_state=42, solver='lbfgs')

    component.setup(INSTANCE)


Evaluation (``evaluation.py``)
''''''''''''''''''''''''''''''

Definition of model evaluation strategy for both the development and production lifecycle.

The evaluation strategy again needs to be submitted to the framework using the ``forml.project.component.setup()``
handler::

    from sklearn import model_selection, metrics
    from forml.project import component
    from forml.stdlib.operator.folding import evaluation
    INSTANCE = evaluation.MergingScorer(
        crossvalidator=model_selection.StratifiedKFold(n_splits=2, shuffle=True, random_state=42),
        metric=metrics.log_loss)

    component.setup(INSTANCE)


Producer Expression (``source.py``)
'''''''''''''''''''''''''''''''''''

Project allows to define the ETL process sourcing the data into the system using a *Producer Expression*. This mechanism
would still be fairly abstract from a physical data source as that's something that would supply a particular *Runtime*.

This part is not fully implemented yet.

Tests
'''''

ForML has an operator unit testing facility (see the :doc:`testing` sections) which can be integrated into the usual
``tests/`` project structure.
