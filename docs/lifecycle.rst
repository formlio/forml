Lifecycle
=========

Machine learning projects are operated in typical modes that are followed in particular order. This pattern is what we
call a lifecycle. ForML supports two specific lifecycles depending on the state of the project.

Development Lifecycle
---------------------

This lifecycle is typically followed during the project development. All work is done in scope of the project source
code working copy and no persistent modules are produced upon execution. It is typically managed using the
``python setup.py <mode>`` interface or in special case using the :doc:`interactive`. This lifecycle is supposed to aid
the development process allowing to quickly see the effect of the project changes.

The expected behavior of the particular mode depends on correct project setup as per the :doc:`project` sections.

The modes of a research lifecycle are:

Test
    Simply run through the unit tests defined as per the :doc:`testing` framework.

    Example::

        $ python3 setup.py test

Score
    Perform a crossvalidation based on the specs defined in ``evaluation.py`` and return the score. One of the potential
    usecases might be a CI integration to continuously monitor (evaluate) the changes in the project development.

    Example::

        $ python3 setup.py score

Tune
    Run hyper-parameter tuning reporting the results (not implemented yet).

    Example::

        $ python3 setup.py tune

Train
    Run the pipeline in the standard train mode. This will produce all the defined models but since it wont persist
    them, this mode is useful merely for testing the training (or displaying the task graph on the :doc:`graphviz`).

    Example::

        $ python3 setup.py train

Package
    Create the distributable project artifact containing all of its dependencies (produced into the ``dist`` directory
    under the project root directory).

    Example::

        $ python3 setup.py bdist_4ml

Upload
    Build and wrap the project into a runable *Artifact* producing a new *Lineage* (that can then be used within
    the *Production Lifecycle*) and upload it to a persistent registry.

    .. note::
       Each particular registry allows uploading only distinct monotonically increasing lineages per any given project,
       hence executing this stage twice against the same registry without incrementing the project version will fail.

    Example::

        $ python3 setup.py bdist_4ml upload


Production Lifecycle
--------------------

After publishing a project lineage into a registry using the ``upload`` mode of the *research lifecycle*, the project
becomes available for the *production lifecycle*. Contrary to the research, this production lifecycle no longer needs
the project source code working copy as it operates solely on the published artifact plus potentially previously
persisted model generations.

The production lifecycle is operated using the CLI (see :doc:`runtime` for full synopsis) and offers the following
modes:

Train
    Fit (incrementally) the stateful parts of the pipeline using new labelled data producing a new *Generation* of
    the given lineage (unless explicit, the default lineage is the one with highest version).

    Example::

        forml train titanic

Tune
    Run hyper-parameter tuning of the selected pipeline and produce new *generation* (not implemented yet).

    Example::

        forml tune titanic

Apply
    Run unlabelled data through a project *generation* (unless explicit, the default generation is the one with highest
    version) producing transformed output (ie *predictions*).

    Example::

        forml apply titanic

Score
    Evaluate the metrics and insights defined in ``evaluation.py`` (not implemented yet).

    Example::

        forml score titanic
