Formalization
=============

First of the main areas covered by ForML is an interface for implementing ML solutions using an unified highlevel API.
 

Project Component Structure
---------------------------

A project is defined as a set of specific components wrapped as python package with a typical ``setuptools`` layout.
ForML offers the *Convention over Configuration* approach for organizing the internal package structure which means it
automatically discovers and imports the expected project components if the author follows this convention (there is
still an option to ignore the convention but the author is then responsible for configuring all the otherwise automatic
steps).

The convention is based simply on defining python *modules* (or *packages*) with expected names within the project
namespace root. ForML doesn't care whether the component is defined as a module (a file with ``.py`` suffix) or package
(a subdirectory with ``__init__.py`` file in it) since both have same import syntax.

This naming convention for the different project components are described in the following subsections. The general
project component structure wrapped within the python application layout might look something like this:

.. code-block::

    myprj
      |- setup.py
      |- src
      |    \- <project_namespace1>
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
--------------------------

Pipeline definition is the most complex part of the project component structure. The framework needs to understand the
pipeline as a *Directed Acyclic Task Dependency Graph*. For this purpose it comes with a concept of *Operators* of
specific types that the user is supplying with actual functionality (ie feature transformer, classifier) and *composing*
together to define the final pipeline. This might look like annoying boilerplate but we believe that's the best way
to give the user full flexibility to implement whatever ML solution while allowing the framework to see the pipeline
still as a granular graph so it can control its runtime aspects.

Standard ML entities like *transformers* or *estimators* can be turned into operators easily by wrapping it with a
provided decorator or adding a provided mixin class into the class hierarchy. More complex entities like for example
a *stacked ensembler* need to be implemented as operators from scratch (reusable entities can be maintained centrally as
library operators).

Pipeline for supervised learning project has typically two modes - *learning* and *applying* (also known as *training*
or *fitting* and *predicting* or *transforming*). This needs to be reflected in the duality of the *Operator* concept
modes as well.

Apart from the two modes there needs to be a syntax for indicating how and what parts of the pipeline should be
optimized using hyperparameter tuning.


Evaluation (``evaluation.py``)
------------------------------

Definition of model evaluation strategy for both the development mode (scoring developed model) or production
evaluation of delivered predictions.


Producer Expression (``source.py``)
-----------------------------------

Project allows to define the ETL process sourcing the data into the system using a *Producer Expression*. This mechanism
would still be fairly abstract from a physical data source as that's something that would supply a particular *Runtime*.
