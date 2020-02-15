Runtime
=======

Runtime is a combination of three abstract concepts: *Runner*, *Registry* and an *IO Engine* (see bellow). These

Having the Runtime components decoupled from the generic framework provides extra level of robustness making
these concept easily adaptable to different execution environments or the custom needs of particular user architectures.

CLI
---

The ForML lifecycle management can be fully operated from command-line using following syntax:

.. code-block::

    Usage: forml [--runtime <name> [--engine <name>]] <command> [options]

    Commands:
        score <project> <generation> [<common options>]
             Crossvalidate using given dataset and return the score.

        tune <project> <generation> [<common options>]
             Perform hyper-parameter tuning and produce new instance.

        train <project> <generation> [<common options>]
            Train the pipeline and produce new instance.

        apply <project> <generation> [<common options>]
            Run the pipeline in apply mode generating predictions/transformations.

    Common Options:
        --etl <name>        Select project defined producer expressions (required if multiple defined).
        --lower <ordinal>   Optional lower bound ordinal used for batch operations.
        --upper <ordinal    Optional upper bound ordinal used for batch operations.


Setup.py
--------


Runner
======

Specific implementation of a system that can execute the lifecycle of a project based on its description (its *Task
Dependency Graph* in particular) is called the *Runner*.

First Runner implementations are:

* Dask
* Graphviz (just to render the task dependency graph vizualization)


Registry
========


IO Engine
=========

IO Engines are Runtime Addons (at least one should be present before the Runtime can be practically used) that can
interpret the project defined *Producer Expressions* and feed the Pipeline with some data.
