ForML Example Project Implementing the Titanic Solution
=======================================================

Quick Start
-----------

To see ForML in action, try the following steps that will lead you through some of the typical usecases:

1. Install the latest version of ``forml``.
2. Explore the project components under the ``src/titanic/`` and the unit tests under ``tests/``.
3. Execute some of the *development lifecycle* modes:

   a. Try running the ``train`` mode on the ``graphviz`` runner::

       $ python3 setup.py train --runer graphviz

   b. Run the ``score`` mode on the (default) ``dask`` runner to get the cross-validation score::

       $ python3 setup.py score
       0.6531806857218416

   c. Create the project package artifact and upload it to the (default) local registry (assuming the same linage
      doesn't already exist - otherwise increment the project version in the ``setup.py``)::

       $ python3 setup.py bdist_4ml upload

      This should publish the project into your local forml registry making it available for the production lifecycle.

4. Execute some of the *production lifecycle* modes:

   a. List the local registry confirming the project has been published::

       $ forml list
       forml-example-titanic
       $ forml list forml-example-titanic
       0.1.dev0
       $ forml list forml-example-titanic 0.1.dev0

      The output shows the project artifact is available in the registry as a lineage 0.1.dev0 not having any generation
      yet (the last command not producing any output).

   b. Train the project to create the first generation of its models and list the registry to confirm it got
      persisted::

       $ forml train forml-example-titanic
       $ forml list forml-example-titanic 0.1.dev0
       1

      Now we have our first generation of the titanic models available in the registry.

   c. Apply the trained generation of the project to get the predictions::

       $ forml apply forml-example-titanic
       [[0.59180614 0.40819386]
       [0.60498469 0.39501531]
       ...
       [0.61020908 0.38979092]
       [0.64064548 0.35935452]]

   d. Run the ``apply`` mode alternatively on the ``graphviz`` runner to explore its task graph::

       $ forml -R graphviz apply forml-example-titanic


Jupyter Tutorial
----------------

See the [notebooks/tutorial.ipynb](notebooks/tutorial.ipynb) for simple tutorial of ForML Jupyter integration.
