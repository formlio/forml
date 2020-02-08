.. forml documentation master file

ForML Documentation
=====================

ForML is a framework for researching, implementing and operating machine learning projects.

Use ForML to formally describe a machine learning problem as a composition of high-level operators.
ForML expands your project into a task dependency graph specific to given life-cycle phase and executes it
using one of its supported runners.

When machine learning projects are described using formal expressions, they become more servisable, extensible,
reproducible, and collaborative.


Principles
----------

It is a comprehension,
management and execution tool taking care of the following three main areas of any such project:

- **Formalization**: ForML uses a formal project component structure and expression interface for high-level project description.
- **Lifecycle**: ForML consistently takes care of all stages of typical ML project lifecycle.
- **Runtime**: ForML provides several means of executing the project integrating with established 3rd party data processing systems.


Content
-------

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Getting Started

   installation
   formalization
   lifecycle

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Architecture

   pipeline
   operator
   runtime
   registry
   engine
