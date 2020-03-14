Principles
==========

Formalization
-------------

Formalization is the prime concept ForML is built upon. Having a common *component structure* for ML projects
and an *expression interface* for their workflows leads to a cleaner implementation that is easier to maintain,
extend or share.


Workflow Expression Interface
    ForML provides convenient interface for describing the project workflow using a high-level expressions for
    *operator compositions*. This makes the definition very clean with all the main complexity being carried out in
    lower layers (the *Operators* and *Actors* as explained in the :doc:`workflow` sections).

    Example of simple workflow::

        FLOW = SimpleImputer(strategy='mean') >> LogisticRegression(max_iter=3, solver='lbfgs')

Project Component Structure
    ForML also introduces its convention for organizing machine learning projects on the module level. This is to have
    some common basic structure that can be understood across projects and help (not only) ForML itself to understand
    the project just by touching the expected structures.

    There should still be enough flexibility to satisfy specific needs of any project. More details explained in the
    :doc:`project` sections.

Operation
---------

ForML identifies several different modes the workflow is supposed to be executed in depending on its given stage.
Set of typical transitions between these stages is what defines particular *lifecycle*.

There are two different lifecycles depending on the *development* versus *production* state of the project. Both of
these are discussed closely in the :doc:`lifecycle` sections.

Composition
-----------

Operator composition is another powerful concept built into ForML. It is the composition in
the `mathematical sense <https://en.wikipedia.org/wiki/Function_composition>`_ that allows to expand the task graph
topology into a complex layout just by simple combination of two operators. More details about composition is discussed
in the :doc:`operator` sections.

Persistence
-----------

Fundamental aspect of the lifecycle is pipeline state transition occurring during *train* and *tune* stages. Each of
these transitions produces a new *Generation*. Generations based on same build belong to one *Lineage*.

Both Lineages and Generations are distinguished by their incremental version numbers establishing a pipeline versioning
schema of ``<lineage_version>-<generation_version>``.

Particular :doc:`runtime` implementations provide a mechanism for Lineage/Generation persistence - a *Registry*
that allows publishing, locating and fetching these instances.
