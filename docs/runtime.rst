Runtime
=======

Runtime is a combination of three abstract concepts: *Runner*, *Registry* and an *IO Engine* (see bellow).

Having the Runtime components decoupled from the generic framework provides extra level of robustness making
these concept easily adaptable to different execution environments or the custom needs of particular user architectures.

CLI
---

The production :doc:`lifecycle` management can be fully operated from command-line using the following syntax:

.. code-block:: none

    usage: forml [-h] [-C CONFIG] [-P REGISTRY] [-R RUNNER] [-E ENGINE]
                 {init,list,tune,train,apply} ...

    Lifecycle Management for Datascience Projects

    positional arguments:
      {init,list,tune,train,apply}
                            program subcommands (-h for individual description)
        init                create skeleton for a new project
        list                show the content of the selected registry
        tune                tune the given project lineage producing new
                            generation
        train               train new generation of given project lineage
        apply               apply given generation of given project lineage

    optional arguments:
      -h, --help            show this help message and exit
      -C CONFIG, --config CONFIG
                            additional config file
      -P REGISTRY, --registry REGISTRY
                            persistent registry reference
      -R RUNNER, --runner RUNNER
                            runtime runner reference
      -E ENGINE, --engine ENGINE
                            IO engine reference


Runner
------

Specific implementation of a system that can execute the lifecycle of a project based on its description (its *Task
Dependency Graph* in particular) is called the *Runner*.

Existing runner implementations are:

* :doc:`dask`
* :doc:`graphviz` (just to render the task dependency graph vizualization)


Registry
--------

The *production lifecycle* uses the model registry for storing artifacts of project lineages as well as
the models of its generations.

ForML can use multiple registries built upon different technologies. The available registry implementations are:

* *Virtual Registry* is a special type of non-persistent registry used within the *Development lifecycle*
* :doc:`filesystem`

IO Engine
---------

IO Engines are Runtime Addons (at least one should be present before the Runtime can be practically used) that can
interpret the project defined *Producer Expressions* and feed the Pipeline with some data.

This part is not fully implemented yet.
