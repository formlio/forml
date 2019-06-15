ForML is a framework for researching, implementing and operating machine learning projects. It is a comprehension,
management and execution tool taking care of the following three main areas of any such project:

* _Formalized description_ of project component structure using an uniformed syntax   
* Identification and management of all stages of typical _ML project lifecycle_
* Project _runtime layer_ for all processing aspects

ForML attempts to abstract the core concept from particular technologies to allow engagement of arbitrary existing
tools and environments both on the formalization level (custom DSL) as well as the execution level (support for
independent runtime implementations). 

To prove the concept within limited time the project restricts itself within following simplification constraints:
* Despite the ambitions of technology independence the POC implementation will stick with the _Python ecosystem_
* Out of the different types of ML problems the POC will focus on _Supervised Learning_


Formalization
=============

First of the main areas covered by ForML is an interface for implementing ML solutions using an unified highlevel
syntax. This would ideally be a custom DSL but given the simplification constraint we stick with a Python based API.
 

Project Component Structure
---------------------------

A project is defined as a set of specific components wrapped as python package with a typical `setuptools` layout.
ForML offers the _Convention over Configuration_ approach for organizing the internal package structure which means it
automatically discovers and imports the expected project components if the author follows this convention (there is
still an option to ignore the convention but the author is then responsible for configuring all the otherwise automatic
steps).

The convention is based simply on defining python _modules_ (or _packages_) with expected names within the project
namespace root. ForML doesn't care whether the component is defined as a module (a file with `.py` suffix) or package
(a subdirectory with `__init__.py` file in it) since both have same import syntax.

This naming convention for the different project components are described in the following subsections. The general
project component structure wrapped within the python application layout might look something like this:

```
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
  |               |- schedule.py
  |- tests
  |    |- sample.sqlite
  |    \- ...
  |- README.md
  \- LICENSE
```


Pipeline (`pipeline.py`)
------------------------

Pipeline definition is the most complex part of the project component structure. The framework needs to understand the
pipeline as a _Directed Acyclic Task Dependency Graph_. For this purpose it comes with a concept of _Operators_ of
specific types that the user is supplying with actual functionality (ie feature transformer, classifier) and _composing_
together to define the final pipeline. This might look like annoying boilerplate but we believe that's the best way
to give the user full flexibility to implement whatever ML solution while allowing the framework to see the pipeline
still as a granular graph so it can control its runtime aspects.

Standard ML entities like _transformers_ or _estimators_ can be turned into operators easily by wrapping it with a
provided decorator or adding a provided mixin class into the class hierarchy. More complex entities like for example
a stacked ensembler need to be implemented as operators from scratch (reusable entities can be maintained centrally as 
library operators).

Pipeline for supervised learning project has typically two modes - _learning_ and _applying_ (also known as _fitting_
and _predicting_ or _transforming_). This needs to be reflected in the duality of the _Operator_ concept modes as well.

Apart from the two modes there needs to be a syntax for indicating how and what parts of the pipeline should be
optimized using hyperparameter tuning.


Evaluation (`evaluation.py`)
----------------------------

Definition of model evaluation strategy for both the development mode (scoring developed model) or production
evaluation of delivered predictions.


Producer Expression (`source.py`)
------------------------------

Project allows to define the ETL process sourcing the data into the system using a _Producer Expression_. This mechanism
would still be fairly abstract from a physical data source as that's something that would supply a particular _Runtime_.

This approach comes from the concept of _Data Producers_ splitting it into the abstract part
(_Producer Expression_ defined as a _Project Component_) and it's physical part (_IO Engine_ defined as part of
a _Runtime_).  

There might possibly be separate expressions for individual lifecycle stages or even several expressions for same stage
(just one would need to be selected during launching).


Schedule (`schedule.py`)
------------------------

User can programatically define a schedule for training, tuning and reporting. The schedule can be based not only on
time periods but also on performance evaluation results. It is the responsibility of the particular runtime to implement
the scheduling as requested. 


Lifecycle
=========

Once the project component structure is defined it can execute its lifecycle stages.

Research Lifecycle
------------------

At this stage the project is being developed, no models are really produced, ale exectuion happens only within the
project scope.

* **Score** - perform a crossvalidation based on the specs defined in `evaluation.py` and return the score; one of the
usecases is a CI integration to continuously monitor (evaluate) the changes in the project
development. 
* **Tune** - run hyper-parameter tuning reporting the results
* **Build** - build and wrap the project into a runable _Artifact_ producing a new _Lineage_ that can be used within
the _Production Lifecycle_.


Production Lifecycle
--------------------

This is typically controlled using the CLI and depends implementation-wise on specific _Runtime_. It is based on a
pipeline _Artifact_ of specific _Lineage_ built out of the _Research Lifecycle_. 

* **Train** - fit (incrementally) the stateful parts of the pipeline using new labelled data producing a new
_Generation_
* **Tune** - run hyper-parameter tuning of the selected pipeline and produce new _Generation_
* **Apply** - run unlabelled data through a pipeline _Generation_ producing transformed output (most typically
_predictions_); the interface mechanism is again Runtime specific (ie a synchronous REST service or async Kafka
consumer-producer etc) 
* **Score** - evaluate the metrics and insights defined in `evaluation.py` and publish them in a way specific to given
Runtime (ie some dashboard)


Instance Persistence
--------------------

Fundamental aspect of the lifecycle is pipeline state transition occurring during _train_ and _tune_ stages. Each of
these transitions produces a new _Generation_. Generations based on same build belong to one _Lineage_.

Both Lineages and Generations are distinguished by their incremental version numbers establishing a pipeline versioning
schema of `<lineage version>.<generation version>`. 

Particular _Runtime_ implementations need to provide a mechanism for Generation persistence - some kind of repository
that allows publishing, locating and fetching these instances.


Runtime
=======

Specific implementation of a system that can execute the lifecycle of a project based on its description (its _Task
Dependency Graph_ in particular) is called the _Runtime_. Having the Runtime decoupled from the generic framework
provides extra level of robustness making the concept easily adaptable to different execution environments or the custom
needs of particular user architectures.

Actual Runtime itself can further be made extensible by external _IO Engines_ that can be installed as addons.

First Runtime implementations are:

* Dask
* Graphviz (just to render the task dependency graph vizualization)


CLI
---

The ForML lifecycle management can be fully operated from command-line using following syntax:

```bash

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
```


IO Engine
---------

IO Engines are Runtime Addons (at least one should be present before the Runtime can be practically used) that can
interpret the project defined _Producer Expressions_ and feed the Pipeline with some data.

There are several different types of IO Engines:
* batch
* stream
    * asynchronous
    * synchronous
