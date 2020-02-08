Lifecycle
=========

Once the project component structure is defined it can execute its lifecycle stages.

Research Lifecycle
------------------

At this stage the project is being developed, no models are really produced, ale exectuion happens only within the
project scope.

* **Score** - perform a crossvalidation based on the specs defined in ``evaluation.py`` and return the score; one of the usecases is a CI integration to continuously monitor (evaluate) the changes in the project development.
* **Tune** - run hyper-parameter tuning reporting the results
* **Build** - build and wrap the project into a runable *Artifact* producing a new *Lineage* that can be used within the *Production Lifecycle*.


Production Lifecycle
--------------------

This is typically controlled using the CLI and depends implementation-wise on specific *Runtime*. It is based on a
pipeline *Artifact* of specific *Lineage* built out of the *Research Lifecycle*.

* **Train** - fit (incrementally) the stateful parts of the pipeline using new labelled data producing a new *Generation*
* **Tune** - run hyper-parameter tuning of the selected pipeline and produce new *Generation*
* **Apply** - run unlabelled data through a pipeline *Generation* producing transformed output (most typically *predictions*); the interface mechanism is again Runtime specific (ie a synchronous REST service or async Kafka consumer-producer etc)
* **Score** - evaluate the metrics and insights defined in ``evaluation.py`` and publish them in a way specific to given Runtime (ie some dashboard)


Instance Persistence
--------------------

Fundamental aspect of the lifecycle is pipeline state transition occurring during *train* and *tune* stages. Each of
these transitions produces a new *Generation*. Generations based on same build belong to one *Lineage*.

Both Lineages and Generations are distinguished by their incremental version numbers establishing a pipeline versioning
schema of ``<lineage version>.<generation version>``.

Particular *Runtime* implementations need to provide a mechanism for Generation persistence - a repository
that allows publishing, locating and fetching these instances.
