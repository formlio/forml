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
  |    \- <optional_namespace1>
  |         \- <optional_namespaceN>
  |              \- myprj
  |                   |- __init__.py
  |                   |- pipeline  # here the component is a package 
  |                   |    |- __init__.py
  |                   |    |- <moduleX>.py  # arbitrary user defined module
  |                   |    \- <moduleY>.py
  |                   |- crossval.py  # here the component is just a module
  |                   |- schedule.py
  |                   |- label.py
  |                   |- source.py
  |                   \- report.py
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

**Discussion:**
It is an open question whether the framework should provide higher granularity in the convention of describing
particular parts of the pipeline (ie `feature_engineering.py`, `feature_selection.py`, `ensembling.py`, `tuning.py`,
...) and letting the tool to compose the pipeline of it in a standard way or whether the convention should end just on
the pipeline level itself letting the user to define it as a whole. The first approach would help structuring the
project in standardized way and would help removing some boilerplate but is probably making too strong assumptions on
how relevant are these steps in general and also whether there is anything like _"standard way"_ of composing a pipeline
from them.


Label Extraction (`label.py`)
-----------------------------

This is actually one of the components in question discussed in the previous paragraph - should it be treated in the
framework as a separate topic or should it just be left to the user to build it into the main pipeline if necessary?
Label extraction looks to be more settled procedure (in terms of it's position in the pipeline) to have its own
component in the ForML project structure.


Cross Validation (`crossval.py`)
--------------------------------

Definition of cross-validation iterator that is used to iteratively split the data whenever it needs to do
crossvalidation (CI/benchmarking, tuning, ensembling).


Producer Expression (`source.py`)
------------------------------

Project allows to define the ETL process sourcing the data into the system using a _Producer Expression_. This mechanism
would still be fairly abstract from a physical data source as that's something that would supply a particular _Runtime_.

This approach comes from the concept of _Data Producers_ splitting it into the abstract part
(_Producer Expression_ defined as a _Project Component_) and it's physical part (_IO Engine_ defined as part of
a _Runtime_).  

There might possibly be separate expressions for individual lifecycle stages or even several expressions for same stage
(just one would need to be selected during launching).

Producer Expressions are implemented on top of _SQLAlchemy selectables_.


Schedule (`schedule.py`)
------------------------

User can programatically define a schedule for training, tuning and reporting. The schedule can be based not only on
time periods but also on performance evaluation results. It is the responsibility of the particular runtime to implement
the scheduling as requested. 


Reporting (`report.py`)
-----------------------

Definition of metrics and insights to be regularly evaluated and reported by means of the particular runtime. This can
be typical machine-learning metrics but possibly also more generic data stats.


Lifecycle
=========

Once the project component structure is defined it can execute its lifecycle stages. This is typically controlled using
the CLI and depends implementation-wise on specific _Runtime_.

The typical lifecycle is:

* **Build** - build and wrap the project into a runable _Artifact_ - what that means is totally specific to particular
_Runtime_ (ie producing a Docker image and uploading it to a registry etc); Artifacts of same project are distinguished
by their incremental version numbers
* **Crossval** - perform a crossvalidation based on the specs defined in `crossval.py` using particular Artifact and
return the score; one of the usecases is a CI integration to continuously monitor (evaluate) the changes in the project
development. 
* **Tune** - run hyper-parameter tuning of the selected pipeline _Artifact_ parameters and produce new _Instance_ (that
get's persisted in a way specific to given Runtime)
* **Train** - fit (incrementally) the stateful parts of the pipeline _Artifact_ using new labelled data producing a new
_Instance_
* **Apply** - run unlabelled data through a pipeline _Instance_ producing transformed output (most typically
_predictions_); the interface mechanism is again Runtime specific (ie a synchronous REST service or async Kafka
consumer-producer etc) 
* **Report** - evaluate the metrics and insights defined in `report.py` and publish them in a way specific to given
Runtime (ie some dashboard)


Instance Persistence
--------------------

Fundamental aspect of the lifecycle is pipeline state transition occurring during _train_ and _tune_ stages. Each of
these transitions produces a new _Instance_.

Instances are distinguished by their incremental version numbers. Since Artifacts themselves are versioned (incremented
by every _build_), the instance version number consists of `<artifact (build) version>.<instance (transition) version>`. 

Particular _Runtime_ implementations need to provide a mechanism for Instance persistence - some kind of repository that
allows publishing, locating and fetching these instances.

Physical format of an Instance is again Runtime-specific but on the abstract level it needs to carry:

* project reference
* artifact (build) version
* instance (transition) version
* training history: 
    * last training timestamp
    * last trained data ordinal number (ie time)
* tuning history (last tune timestamp)
* all state information

Since the format used for Instance persistence is Runtime-specific, an Instance produced by particular Runtime might not
be compatible with another Runtime. 


Runtime
=======

Specific implementation of a system that can execute the lifecycle of a project based on its description (its _Task
Dependency Graph_ in particular) is called the _Runtime_. Having the Runtime decoupled from the generic framework
provides extra level of robustness making the concept easily adaptable to different execution environments or the custom
needs of particular user architectures.

Actual Runtime itself can further be made extensible by external _IO Engines_ that can be installed as addons.


Native Runtime
--------------

Native runtime is the reference runtime implementation. It is not targeting large scale production usecases but should
be ideal runtime for executing project CI jobs or local project development.

Native Runtime is taking following approach implementing the abstract ForML concept:

* Python multiprocessing used for distributing the Pipeline on local system (can't be distributed over multiple hosts
though).
* _Instance Persistence_ implemented using a Python Pickling format dumped as a single file into local directory
structure
* Artifacts are plain python (wheel) packages
* Default _IO Engines_ (additional engines can be provided as addons):
    * _synchronous streaming_ implemented as REST service
    * _batch_ implemented on top of local sqlite files
* Any reporting results are sent to stdout
* Scheduling is unsupported


Kube Runtime
------------

This Runtime allows running ForML project at scale on a Kubernetes cluster. It is based on following principles:

* Argo used to map a pipeline into a workflow runable on kubernetes cluster
* _Instance Persistence_ implemented using blob storage (S3, WASB or Minio for non-cloud deployments)
* Artifacts are docker images that can run individual operators as entrypoints and interconnect within the graph across
relevant PODs using TCP.
* Default _IO Engines_ (additional engines can be provided as addons):
    * _synchronous streaming_ implemented as REST service around Argo Events
    * no default _batch_ engines (needs addons)
* Graphana used for visualizing metric reports
* Scheduling is implemented using native Argo capabilities


CLI
---

The ForML lifecycle management can be fully operated from command-line using following syntax:

```bash

Usage: forml [--runtime <name> [--engine <name>]] <command> [options]

Commands:
    build
         Build the project producing an Artifact for given runtime.
    
    score <artifact> [<common options>]
         Crossvalidate using given dataset and return the score.
         
    tune <instance> [<common options>]
         Perform hyper-parameter tuning and produce new instance.
    
    train <instance> [<common options>]
        Train the pipeline and produce new instance.
    
    apply <instance> [<common options>]
        Run the pipeline in apply mode generating predictions/transformations.
    
    report <instance> [<common options>]
        Calculate and publish project performance metric/insights. 
    
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
