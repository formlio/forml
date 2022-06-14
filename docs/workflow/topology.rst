 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
 ..   http://www.apache.org/licenses/LICENSE-2.0
 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Flow Topology
=============

ForML has custom primitives for logical representation of the *task graph*, which also provide
the API for its assembly during the construction phase.

.. note::
    Thanks to this runtime-agnostic internal representation of the task graph, ForML can support
    number of different third-party :doc:`runners <../runner>` simply by converting the DAG on
    demand from its internal structure to the particular representation of the target runtime.


Task Graph Primitives
---------------------

While the actual unit of work - the vertex in the runtime DAG - is a *task* provided as the
:doc:`Actor <actor>` implementation, its logical representation used by ForML internally is the
abstract ``flow.Node`` structure and its subtype ``flow.Worker`` in particular.

Creating a Worker
^^^^^^^^^^^^^^^^^

Worker node gets created simply by providing a ``flow.Builder``
:ref:`actor builder <actor-builder>` and the required number of the input and output (apply) ports:

.. code-block:: python
    :linenos:

    from forml import flow
    from forml.pipeline import payload  # let's use some existing actors

    # one input, one output and the PandasSelect stateless actor builder
    select_foobar = flow.Worker(
        payload.PandasSelect.builder(columns=['foo', 'bar']),
        szin=1,
        szout=1,
    )
    select_bar = flow.Worker(payload.PandasSelect.builder(columns=['bar']), szin=1, szout=1)
    select_baz = flow.Worker(payload.PandasSelect.builder(columns=['baz']), szin=1, szout=1)

    # one input, one output and the PandasDrop stateless actor builder
    drop_bar = flow.Worker(payload.PandasDrop.builder(columns=['bar']), szin=1, szout=1)

    # two inputs, one output and the PandasConcat stateless actor builder
    concat = flow.Worker(payload.PandasConcat.builder(axis='columns'), szin=2, szout=1)

    # one input, one output and the mean_impute stateful actor builder (from the previous chapter)
    impute_baz_apply = flow.Worker(mean_impute.builder(column='foo'), szin=1, szout=1)

This gives us the following disconnected workers:

.. md-mermaid::

    graph LR
        sfbi((i)) --> select_foobar --> sfbo((o))
        szi((i)) --> select_baz --> szo((o))
        ci1((i1)) & ci2((i2)) --> concat --> co((o))
        dbi((i)) --> drop_bar --> dbo((o))
        sri((i)) --> select_bar --> sro((o))
        ibai((i)) --> impute_baz_apply --> ibao((o))

.. note::
    All the actors we chose in this example work with Pandas payload - by no means this is some
    official format required by ForML. As explained, ForML doesn't care about the payload and the
    choice of compatible actors is responsibility of the implementor.

Connecting Nodes
^^^^^^^^^^^^^^^^

Let's now create the actual dependency of the individual tasks by connecting the worker (apply)
ports:

.. code-block:: python

    concat[0].subscribe(select_foobar[0])
    concat[1].subscribe(select_baz[0])
    drop_bar[0].subscribe(concat[0])
    select_bar[0].subscribe(concat[0])
    impute_baz_apply[0].subscribe(drop_bar[0])

The ``node[port_index]`` *getitem* syntax on a ``flow.Node`` instance returns a ``flow.PubSub``
object for the particular :ref:`Apply port <actor-ports>` on the *input* or *output* side
(determined by context) of that node. This can be used to publish or subscribe to another such
object.

.. caution::
    Any input port can be subscribed to at most one upstream output port but any output port can
    be publishing to multiple subscribed input ports. Actor cannot subscribe to itself.

The key method of the ``flow.PubSub`` is the ``.subscribe()``:

.. automethod:: forml.flow.PubSub.subscribe


Now, with that connections between our nodes, the topology looks shown:

.. md-mermaid::

    graph LR
        sfbi((i)) --> select_foobar -- "o(0)->i(0)" --> concat
        sbi((i)) --> select_baz -- "o(0)->i(1)" --> concat
        concat -- "o(0)->i(0)" --> drop_bar -- "o(0)->i(0)" --> impute_baz_apply --> ibao((o))
        concat -- "o(0)->i(0)" --> select_bar --> sro((o))


.. _topology-state:

Dealing with Worker State
^^^^^^^^^^^^^^^^^^^^^^^^^

Sofar we've discussed only the *apply-mode* connections. For *stateful* nodes (i.e. nodes
representing :doc:`stateful actors <actor>`), we also need to take care of the *train-mode*
connections to their *Train* and *Label* ports. This is achieved simply by using the ``.train()``
method on the worker object:

.. automethod:: forml.flow.Worker.train

Training and applying even the same worker are two distinct tasks, hence they need to be
represented using two related but separate worker nodes. ForML transparently manages these
related workers using a ``flow.Worker.Group`` instance. All workers in the same *group* have the
same shape and share the same :ref:`actor builder <actor-builder>` instance.

Based on the group membership (and the general context), ForML automatically handles the runtime
state management between the different modes of the same actor (the :ref:`State ports
<actor-ports>` are *system* level ports and cannot be connected from the user level API).

Workers of the same group can be created using one of the two methods:

.. automethod:: forml.flow.Worker.fork
.. automethod:: forml.flow.Worker.fgen


.. code-block:: python

    impute_baz_train = impute_baz_apply.fork()
    impute_baz_train.train(drop_bar[0], select_bar[0])

Now we have one more worker node ``impute_baz_train`` logically *grouped* as a companion of the
original ``impute_baz_apply``. The task graph now looks like this:

.. md-mermaid::

    graph LR
        subgraph Group
        impute_baz_apply
        impute_baz_train
        end
        sfbi((i)) --> select_foobar -- "o(0)->i(0)" --> concat
        sbi((i)) --> select_baz -- "o(0)->i(1)" --> concat
        concat -- "o(0)->i(0)" --> drop_bar -- "o(0)->i(0)" --> impute_baz_apply --> ibao((o))
        concat -- "o(0)->i(0)" --> select_bar -- "o(0)->Label" --> impute_baz_train
        drop_bar -- "o(0)->Train" --> impute_baz_train


.. caution::
    Worker groups and the trained workers impose a couple of additional constraints:

    * At most one worker in the same group can be trained.
    * Either both *Train* and *Label* or all *Apply* input and output ports of each worker must
      be connected.

.. _topology-future:

Future Nodes
^^^^^^^^^^^^

In addition to the *Worker* nodes, there is a special node implementation called ``flow.Future``
representing a future worker placeholder. *Future* can be used during topology construction when
the real connected worker-to-be is not know yet (e.g. when implementing an
:doc:`operator <operator>` which doesn't know what up/down stream workers will it be eventually
composed with). When connected to a real worker, the Future node will automatically collapse and
disappear from the topology.

The following example demonstrates the functionality of the *Future* nodes:

.. code-block:: python
    :linenos:

    from forml import flow

    worker1 = flow.Worker(SomeActor.builder(), szin=1, szout=1)
    worker2 = flow.Worker(AnotherActor.builder(), szin=1, szout=1)
    future1 = flow.Future()  # defaults to szin=1, szout=1 (other shapes still possible)
    future2 = flow.Future()

    future1[0].subscribe(worker1[0])
    worker2[0].subscribe(future2[0])

.. md-mermaid::

    graph LR
        worker1 --> f1((future1))
        f2((future2)) --> worker2


As the diagram shows, we have ``worker1`` node connecting its first *apply* output port to the
``future1`` *Future* node and another ``future2`` connected to input port of ``worker2``. Now,
after subscribing the ``future2`` node to the ``future1`` output, you can see how both the
*Future* nodes disappear from the topology and the workers become connected directly:

.. code-block:: python

    future2[0].subscribe(future1[0])


.. md-mermaid::

    graph LR
        worker1 --> worker2


.. warning::
    Flow containing *Future* nodes is considered *incomplete* and cannot be passed for execution
    until all Future nodes are collapsed.

.. _topology-logical:

Logical Structures
------------------

When implementing more complex topologies (typically in scope of :doc:`operators development
<operator>`), the significant parts of the task graph become its *entry* and *exit* nodes (as
that's where new connections are being added), while the inner nodes (already fully connected)
fade away from the perspective of the ongoing construction.

For this purpose, ForML uses the ``flow.Segment`` abstraction representing a subgraph between one
entry (``.head``) and exit (``.tail``) node and providing a useful API to work with this part of
the task graph:

.. autoclass:: forml.flow.Segment
   :members: publisher, subscribe, extend, copy

.. caution::
    Note the ``.head`` node must have *single input port* and the ``.tail`` node must have
    *single output port*.

To carry one of the core ForML traits - the inseparability of the *train* and *apply* mode
implementations - ForML uses the ``flow.Trunk`` structure as the integrated representation of the
related *segments*. There are actually three segments that need to be bound together to cover all
the matching tasks across the different modes. While the *apply* mode is represented by its
single segment, the *train* mode needs in addition to its *train* segment also a dedicated
segment for its *label* task graph.

.. autoclass:: forml.flow.Trunk
   :members: extend, use


Compiler
--------

While representing the task graph using linked structures is practical for implementing the
user-level API, more efficient structure for its actual runtime execution is the (actor)
*adjacency matrix* produced by the internal ForML compiler.

ForML uses its compiler to:

#. Augment the task graph by adding any necessary system-level nodes (e.g. to automatically
   manage persistence of any :doc:`stateful actors <actor>`).
#. Optimizing the task graph by removing any irrelevant or redundant parts.
#. Generating the adjacency matrix representation suitable for runtime execution.


See our existing topology enhanced by the compiler with adding the state *Dumper* and *Loader*
system nodes plus connecting the relevant *State* ports (dotted lines):

.. md-mermaid::

    graph LR
        subgraph Group
        impute_baz_apply
        impute_baz_train
        end
        sfbi((i)) --> select_foobar -- "o(0)->i(0)" --> concat
        sbi((i)) --> select_baz -- "o(0)->i(1)" --> concat
        concat -- "o(0)->i(0)" --> drop_bar -- "o(0)->i(0)" --> impute_baz_apply --> ibao((o))
        concat -- "o(0)->i(0)" --> select_bar -- "o(0)->Label" --> impute_baz_train
        drop_bar -- "o(0)->Train" --> impute_baz_train
        impute_baz_train -. State .-> impute_baz_apply
        l[(Loader)] -. State .-> impute_baz_train -. State .-> d[(Dumper)]
