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
Pyfunc runner.
"""
import abc
import collections
import logging
import typing

import forml
from forml import flow, io
from forml.io import dsl, layout
from forml.runtime import asset, code, facility

LOGGER = logging.getLogger(__name__)


class Term(abc.ABC):
    """Base class for lambda terms."""

    @abc.abstractmethod
    def __call__(self, arg: typing.Any) -> typing.Any:
        """Term body."""


class Task(Term):
    """Term representing an actor action."""

    def __init__(self, actor: flow.Actor, action: code.Mapper):
        self._actor: flow.Actor = actor
        self._action: code.Mapper = action

    def __call__(self, arg: typing.Any) -> typing.Any:
        return self._action(self._actor, arg)


class Get(Term):
    """Simple sequence index getter."""

    def __init__(self, index: int):
        self._index: int = index

    def __call__(self, arg: typing.Any) -> typing.Any:
        return arg[self._index]


class Chain(Term):
    """Linear sequence of terms to be called."""

    def __init__(self, right: Term, left: Term):
        self._right: Term = right
        self._left: Term = left

    def __call__(self, arg: typing.Any) -> typing.Any:
        return self._right(self._left(arg))


class Zip(Term):
    """Term involving multiple inputs."""

    class Push(Term):
        """Helper term for producing value replicas to make them available in parallel branches."""

        def __init__(self, queue: collections.deque[typing.Any], replicas: int):
            assert replicas > 0
            self._queue: collections.deque[typing.Any] = queue
            self._replicas: int = replicas

        def __call__(self, arg: typing.Any) -> typing.Any:
            assert not self._queue, 'Outstanding elements'
            for _ in range(self._replicas):
                self._queue.append(arg)  # assuming we are duplicating just the reference
            return arg

    class Pop(Term):
        """Helper term of accessing the replicated values created in parallel branch."""

        def __init__(self, queue: collections.deque[typing.Any]):
            self._queue: collections.deque[typing.Any] = queue

        def __call__(self, arg: typing.Any) -> typing.Any:
            return self._queue.popleft()

        def __del__(self):
            assert not self._queue, 'Outstanding elements'

    def __init__(self, instruction: code.Instruction, *branches: Term):
        self._instruction: code.Instruction = instruction
        self._branches: tuple[Term] = branches

    def __call__(self, arg: typing.Any) -> typing.Any:
        return self._instruction(*(b(arg) for b in self._branches))

    @classmethod
    def fork(cls, term: Term, szout: int = 1) -> typing.Iterable[Term]:
        """Method for creating a sequence of terms implementing the forking strategy."""
        if szout > 1:
            replicas = szout - 1
            queue = collections.deque(maxlen=replicas)
            return [cls.Push(queue, replicas), *(cls.Pop(queue) for _ in range(replicas))]
        return [term]


class Expression(Term):
    """Final composed lambda expression representing the DAG as a chained function call."""

    class Node(typing.NamedTuple):
        """Helper case class representing DAG node metadata."""

        term: Term
        szout: int
        args: typing.Sequence[Term]

    def __init__(self, symbols: typing.Sequence[code.Symbol]):
        dag = self._build(symbols)
        assert len(dag) > 0 and dag[-1].szout == 0 and not dag[0].args, 'Invalid DAG'
        providers: typing.Mapping[Term, collections.deque[Term]] = collections.defaultdict(
            collections.deque, {dag[0].term: collections.deque([dag[0].term])}
        )
        for node in dag:
            args = [providers[a].popleft() for a in node.args]
            term = (Zip if len(args) > 1 else Chain)(providers[node.term].popleft(), *args)
            providers[node.term].extend(Zip.fork(term, node.szout))
        assert len(providers[dag[-1].term]) == 1
        self._expression: Term = providers[dag[-1].term].popleft()
        assert not any(providers.values()), 'Outstanding providers'

    def __call__(self, arg: typing.Any) -> typing.Any:
        return self._expression(arg)

    @staticmethod
    def _order(
        dag: typing.Mapping[code.Instruction, typing.Sequence[code.Instruction]]
    ) -> typing.Sequence[code.Instruction]:
        """Return the dag nodes ordered from head to tail dependency-wise.

        Args:
            dag: Dag dependency mapping.

        Returns:
            Dag nodes ordered dependency-wise.
        """

        def walk(level: int, *parents: code.Instruction) -> None:
            for node in parents:
                index[node] = max(index[node], level)
                walk(level + 1, *dag[node])

        leaves = set(dag).difference(p for a in dag.values() for p in a)
        assert len(leaves) == 1, 'Expecting single output DAG'
        tail = leaves.pop()
        index: dict[code.Instruction, int] = collections.defaultdict(int, {tail: 0})
        walk(1, *dag[tail])
        return sorted(index, key=lambda i: index[i], reverse=True)

    @classmethod
    def _build(cls, symbols: typing.Iterable[code.Symbol]) -> typing.Sequence['Expression.Node']:
        """Build the ordered DAG sequence of terms.

        Args:
            symbols: Source symbols representing the code to be executed.

        Returns:
            Sequence of tuples each representing a terms, number of its outputs and a sequence of its upstream terms.
        """

        def resolve(source: code.Instruction) -> Term:
            """Get the term instance representing the given instruction and count the number of its usages.

            Args:
                source: Instruction to be mapped

            Returns:
                Mapped target term.
            """
            target = i2t[source]
            szout[target] += 1
            return target

        def evaluate(arg: code.Instruction) -> typing.Any:
            """Attempt to evaluate given instruction if possible, else return the instruction.

            Args:
                arg: Instruction to be evaluated.

            Returns:
                Evaluated or original instruction.
            """
            return arg() if isinstance(arg, code.Loader) else arg

        upstream: dict[code.Instruction, tuple[code.Instruction]] = dict(symbols)
        i2t: dict[code.Instruction, Term] = {}
        dag: list[tuple[Term, tuple[Term]]] = []
        szout: dict[Term, int] = collections.defaultdict(int)
        for instruction in cls._order(upstream):
            assert not isinstance(instruction, (code.Dumper, code.Committer)), f'Unexpected instruction: {instruction}'
            if isinstance(instruction, code.Loader):
                assert not upstream[instruction], f'Dependent loader: {instruction}'
                continue  # just ignore the instruction as we are going to condense it
            if isinstance(instruction, code.Getter):
                args = upstream[instruction]
                term = Get(instruction.index)
            else:
                assert isinstance(instruction, code.Functor), f'Not a functor: {instruction}'
                spec, action = instruction
                actor = spec()
                action, args = action.reduce(actor, *(evaluate(a) for a in upstream[instruction]))
                term = Task(actor, action)
            dag.append((term, tuple(resolve(a) for a in args)))
            i2t[instruction] = term
        return tuple(cls.Node(t, szout[t], u) for t, u in dag)


class Runner(facility.Runner, alias='pyfunc'):
    """Python function based runner implementation."""

    def __init__(
        self,
        instance: typing.Optional[asset.Instance] = None,
        feed: typing.Optional[io.Feed] = None,
        sink: typing.Optional[io.Sink] = None,
    ):
        super().__init__(instance, feed, sink)
        composition = self._build(None, None, self._instance.project.pipeline)
        self._expression = Expression(code.generate(composition.apply, self._instance.state(composition.persistent)))

    def train(self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None) -> None:
        raise forml.InvalidError('Training not supported by this runner')

    def tune(self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None) -> None:
        raise forml.InvalidError('Tuning not supported by this runner')

    def _run(self, symbols: typing.Sequence[code.Symbol]) -> None:
        Expression(symbols)(None)

    def call(self, source: layout.ColumnMajor) -> layout.ColumnMajor:
        """Func exec entrypoint.

        Args:
            source: Input to be sent to the pipeline.

        Returns:
            Pipeline output.
        """
        return self._expression(source)
