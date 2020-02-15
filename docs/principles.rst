Formalization
=============

Formalization is the prime concept ForML is built upon. Having a common *component structure* for ML projects
and an *expression interface* for their workflows leads to a cleaner implementation that is easier to maintain,
extend or share.


Workflow Expression Interface
    ForML provides convenient interface for describing the project workflow using a high-level expressions for
    *operator compositions*. This makes the definition very clean with all the main complexity being carried out in
    lower layers (the *Operators* and *Actors* as explained in the :doc:`workflow` sections). Example of simple workflow::

        FLOW = SimpleImputer(strategy='mean') >> LogisticRegression(max_iter=3, solver='lbfgs')

Project Component Structure
    ForML also introduces its convention for organizing machine learning projects on the module level. This is to have
    some common basic structure that can be understood across projects and help (not only) ForML itself to understand
    the project just by touching the expected structures. There should still be enough flexibility to satisfy specific
    needs of any project. More details explained in the :doc:`project` sections.


Lifecycle
=========

ForML identifies several different modes the workflow is supposed to be executed in depending on its given stage.
Set of typical transitions between these stages is what defines the particular *lifecycle*.

Research Lifecycle
------------------

This lifecycle is typically followed during the project development. No models have been produced yet, all execution
is carried out solely within the project scope. It is typically triggered using the ``python setup.py <mode>`` interface
or :doc:`interactive`.

The stages of a research lifecycle are:

Score
    perform a crossvalidation based on the specs defined in ``evaluation.py`` and return the score; one of the usecases
    is a CI integration to continuously monitor (evaluate) the changes in the project development.
Tune
    run hyper-parameter tuning reporting the results
Build
    build and wrap the project into a runable *Artifact* producing a new *Lineage* that can be used within
    the *Production Lifecycle* (see the Persistence_ section for more details about Lineages).


Production Lifecycle
--------------------

This is typically triggered using the CLI. It is based on a pipeline *Artifact* of specific *Lineage*
built out of the *Research Lifecycle*.

Train
    fit (incrementally) the stateful parts of the pipeline using new labelled data producing a new *Generation*
Tune
    run hyper-parameter tuning of the selected pipeline and produce new *Generation*
Apply
    run unlabelled data through a pipeline *Generation* producing transformed output (most typically *predictions*);
    the interface mechanism is again Runtime specific (ie a synchronous REST service or async Kafka consumer-producer etc)
Score
    evaluate the metrics and insights defined in ``evaluation.py`` and publish them in a way specific to given Runtime
    (ie some dashboard)


Composition
===========

Operator composition is another powerful concept built into ForML. It is the composition in
the `mathematical sense <https://en.wikipedia.org/wiki/Function_composition>`_ that allows to expand the task graph
topology into a complex layout just by simple combination of two operators. More details about composition is discussed
in the :doc:`operator` sections.

Persistence
===========

Fundamental aspect of the lifecycle is pipeline state transition occurring during *train* and *tune* stages. Each of
these transitions produces a new *Generation*. Generations based on same build belong to one *Lineage*.

Both Lineages and Generations are distinguished by their incremental version numbers establishing a pipeline versioning
schema of ``<lineage version>.<generation version>``.

Particular :doc:`runtime` implementations provide a mechanism for Lineage/Generation persistence - a *Registry*
that allows publishing, locating and fetching these instances.
