# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Low-latency function runner suitable for apply-mode serving.
"""
import abc
import collections
import logging
import typing

import forml
from forml import flow, io, runtime
from forml.io import asset, dsl, layout

LOGGER = logging.getLogger(__name__)


class Term(abc.ABC):
    """Base class for lambda terms."""

    @abc.abstractmethod
    def __call__(self, arg: typing.Any) -> typing.Any:
        """Term body."""


class Task(Term):
    """Term representing an actor action."""

    def __init__(self, actor: flow.Actor, action: flow.Apply):
        self._actor: flow.Actor = actor
        self._action: flow.Apply = action

    def __repr__(self):
        return f'{self._actor}.{self._action}'

    def __call__(self, *args: typing.Any) -> typing.Any:
        return self._action(self._actor, *args)


class Get(Term):
    """Simple sequence index getter."""

    def __init__(self, index: int):
        self._index: int = index

    def __repr__(self):
        return f'[{self._index}]'

    def __call__(self, arg: typing.Any) -> typing.Any:
        return arg[self._index]


class Chain(Term):
    """Linear sequence of terms to be called."""

    def __init__(self, right: Term, left: Term):
        self._right: Term = right
        self._left: Term = left

    def __repr__(self):
        return f'{self._right}({self._left})'

    def __call__(self, arg: typing.Any) -> typing.Any:
        return self._right(self._left(arg))


class Zip(Term):
    """Term involving multiple inputs."""

    def __init__(self, instruction: flow.Instruction, *branches: Term):
        self._instruction: flow.Instruction = instruction
        self._branches: tuple[Term] = branches

    def __repr__(self):
        return f'{self._instruction}({", ".join(repr(b) for b in self._branches)})'

    def __call__(self, arg: typing.Any) -> typing.Any:
        return self._instruction(*(b(arg) for b in self._branches))


class Branch(Term, metaclass=abc.ABCMeta):
    """Base class for branch terms."""

    def __init__(self, queue: typing.Deque[typing.Any], name: str):
        self._queue: typing.Deque[typing.Any] = queue
        self._name: str = name

    def __repr__(self):
        return f'{self.__class__.__name__}[{self._name}]'

    @classmethod
    def fork(cls, term: Term, szout: int = 1) -> typing.Iterable[Term]:
        """Method for creating a sequence of terms implementing the forking strategy."""
        if szout > 1:
            replicas = szout - 1
            queue = collections.deque(maxlen=replicas)
            return [Push(queue, term, replicas), *(Pop(queue, repr(term)) for _ in range(replicas))]
        return [term]


class Push(Branch):
    """Helper branch term for producing value replicas to make them available in parallel branches."""

    def __init__(self, queue: typing.Deque[typing.Any], term: Term, replicas: int):
        assert replicas > 0
        super().__init__(queue, repr(term))
        self._term: Term = term
        self._replicas: int = replicas

    def __call__(self, arg: typing.Any) -> typing.Any:
        assert not self._queue, 'Outstanding elements'
        value = self._term(arg)
        for _ in range(self._replicas):
            self._queue.append(value)  # assuming we are duplicating just the reference
        return value


class Pop(Branch):
    """Helper branch term for accessing the replicated values created in parallel branch."""

    def __call__(self, arg: typing.Any) -> typing.Any:
        return self._queue.popleft()

    def __del__(self):
        assert not self._queue, 'Outstanding elements'


class Expression(Term):
    """Final composed lambda expression representing the DAG as a chained function call."""

    class Node(typing.NamedTuple):
        """Helper case class representing DAG node metadata."""

        term: Term
        szout: int
        args: typing.Sequence[Term]

    def __init__(self, symbols: typing.Sequence[flow.Symbol]):
        dag = self._build(symbols)
        assert len(dag) > 0 and dag[-1].szout == 0 and not dag[0].args, 'Invalid DAG'
        providers: typing.Mapping[Term, typing.Deque[Term]] = {n.term: collections.deque([n.term]) for n in dag}

        for node in dag[1:]:
            args = [providers[a].popleft() for a in node.args]
            term = (Zip if len(args) > 1 else Chain)(providers[node.term].popleft(), *args)
            providers[node.term].extend(Branch.fork(term, node.szout))
        assert len(providers[dag[-1].term]) == 1
        self._term: Term = providers[dag[-1].term].popleft()
        assert not any(providers.values()), 'Outstanding providers'

    def __call__(self, arg: typing.Any) -> typing.Any:
        return self._term(arg)

    def __repr__(self):
        return repr(self._term)

    @staticmethod
    def _order(
        dag: typing.Mapping[flow.Instruction, typing.Sequence[flow.Instruction]]
    ) -> typing.Sequence[flow.Instruction]:
        """Return the dag nodes ordered from head to tail dependency-wise.

        Args:
            dag: Dag dependency mapping.

        Returns:
            Dag nodes ordered dependency-wise.
        """

        def walk(level: int, *parents: flow.Instruction) -> None:
            for node in parents:
                index[node] = max(index[node], level)
                walk(level + 1, *dag[node])

        leaves = set(dag).difference(p for a in dag.values() for p in a)
        assert len(leaves) == 1, 'Expecting single output DAG'
        tail = leaves.pop()
        index: dict[flow.Instruction, int] = collections.defaultdict(int, {tail: 0})
        walk(1, *dag[tail])
        return sorted(index, key=lambda i: index[i], reverse=True)

    @classmethod
    def _build(cls, symbols: typing.Iterable[flow.Symbol]) -> typing.Sequence['Expression.Node']:
        """Build the ordered DAG sequence of terms.

        Args:
            symbols: Source symbols representing the code to be executed.

        Returns:
            Sequence of tuples each representing a terms, number of its outputs and a sequence of its upstream terms.
        """

        def resolve(source: flow.Instruction) -> Term:
            """Get the term instance representing the given instruction and count the number of its usages.

            Args:
                source: Instruction to be mapped

            Returns:
                Mapped target term.
            """
            target = i2t[source]
            szout[target] += 1
            return target

        def evaluate(arg: flow.Instruction) -> typing.Any:
            """Attempt to evaluate given instruction if possible, else return the instruction.

            Args:
                arg: Instruction to be evaluated.

            Returns:
                Evaluated or original instruction.
            """
            return arg() if isinstance(arg, flow.Loader) else arg

        upstream: dict[flow.Instruction, tuple[flow.Instruction]] = dict(symbols)
        i2t: dict[flow.Instruction, Term] = {}
        dag: list[tuple[Term, tuple[Term]]] = []
        szout: dict[Term, int] = collections.defaultdict(int)
        for instruction in cls._order(upstream):
            assert not isinstance(instruction, (flow.Dumper, flow.Committer)), f'Unexpected instruction: {instruction}'
            if isinstance(instruction, flow.Loader):
                assert not upstream[instruction], f'Dependent loader: {instruction}'
                continue  # just ignore the instruction as we are going to condense it
            if isinstance(instruction, flow.Getter):
                args = upstream[instruction]
                term = Get(instruction.index)
            else:
                assert isinstance(instruction, flow.Functor), f'Not a functor: {instruction}'
                spec, action = instruction
                actor = spec()
                action, args = action.reduce(actor, *(evaluate(a) for a in upstream[instruction]))
                term = Task(actor, action)
            dag.append((term, tuple(resolve(a) for a in args)))
            i2t[instruction] = term
        return tuple(cls.Node(t, szout[t], u) for t, u in dag)


class Runner(runtime.Runner, alias='pyfunc'):
    """Python function based runner implementation."""

    def __init__(
        self,
        instance: typing.Optional[asset.Instance] = None,
        feed: typing.Optional[io.Feed] = None,
        sink: typing.Optional[io.Sink] = None,
    ):
        super().__init__(instance, feed, sink)
        composition = self._build(None, None, self._instance.project.pipeline)
        self._expression = Expression(flow.generate(composition.apply, self._instance.state(composition.persistent)))

    def train(self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None) -> None:
        raise forml.InvalidError('Invalid runner mode')

    def tune(self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None) -> None:
        raise forml.InvalidError('Invalid runner mode')

    def _run(self, symbols: typing.Sequence[flow.Symbol]) -> None:
        Expression(symbols)(None)

    def call(self, entry: layout.Entry) -> layout.Outcome:
        """Func exec entrypoint.

        Args:
            entry: Input to be sent to the pipeline.

        Returns:
            Pipeline output.
        """
        return self._expression(entry)
