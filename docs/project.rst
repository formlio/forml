Project Component Structure
---------------------------

ForML project is defined as a set of specific components wrapped into a python package with the usual ``setuptools``
layout. The framework offers the *Convention over Configuration* approach for organizing the internal package structure,
which means it automatically discovers the relevant project components if the author follows this convention (there is
still an option to ignore the convention, but the author is then responsible for configuring all the otherwise
automatic steps himself).

The convention is based simply on implementing specific python *modules* (or *packages*) within the project
namespace root. ForML doesn't care whether the component is defined as a module (a file with ``.py`` suffix) or
a package (a subdirectory with ``__init__.py`` file in it) since both have same import syntax.

This naming conventions for the different project components are described in the following subsections. The general
project component structure wrapped within the python application layout might look similar to this:

.. code-block::

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



Pipeline (``pipeline.py``)
''''''''''''''''''''''''''

Pipeline definition is the heart of the project component structure. The framework needs to understand the
pipeline as a *Directed Acyclic Task Dependency Graph*. For this purpose it comes with a concept of *Operators* that
the user is supplying with actual functionality (ie feature transformer, classifier) and *composing* together to
define the final flow of.

Standard ML entities like *transformers* or *estimators* can be turned into operators easily by wrapping them within a
provided decorator or adding a provided mixin class into the class hierarchy. More complex entities like for example
a *stacked ensembler* need to be implemented as operators from scratch (reusable entities can be maintained centrally as
library operators).

Pipeline for supervised learning project has typically two modes - *learning* and *applying* (also known as *training*
or *fitting* and *predicting* or *transforming*). This needs to be reflected in the duality of the *Operator* concept
modes as well.

Apart from the two modes there needs to be a syntax for indicating how and what parts of the pipeline should be
optimized using hyperparameter tuning.


Evaluation (``evaluation.py``)
''''''''''''''''''''''''''''''

Definition of model evaluation strategy for both the development mode (scoring developed model) or production
evaluation of delivered predictions.


Producer Expression (``source.py``)
'''''''''''''''''''''''''''''''''''

Project allows to define the ETL process sourcing the data into the system using a *Producer Expression*. This mechanism
would still be fairly abstract from a physical data source as that's something that would supply a particular *Runtime*.


Tests
'''''

ForML has an operator unit testing facility (see the :doc:`testing` sections) which can be integrated into the usual
``tests/`` project structure.