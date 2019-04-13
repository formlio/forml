"""
Runtime symbols compilation.
"""

import collections
import itertools
import logging
import typing
import uuid

from forml.flow.graph import node as grnode, view
from forml.runtime import assembly
from forml.runtime.assembly import instruction as instmod
from forml.runtime.asset import access


LOGGER = logging.getLogger(__name__)


class Table(view.Visitor, collections.Iterable):
    """Dynamic builder of the runtime symbols. Table uses node UIDs and GIDs where possible as instruction keys.
    """
    class Linkage:
        """Structure for registering instruction dependency tree as relations between target (receiving) instruction
        and its upstream dependency instructions representing its positional arguments.
        """
        def __init__(self):
            self._absolute: typing.Dict[uuid.UUID,
                                        typing.List[typing.Optional[uuid.UUID]]] = collections.defaultdict(list)
            self._prefixed: typing.Dict[uuid.UUID,
                                        typing.List[typing.Optional[uuid.UUID]]] = collections.defaultdict(list)

        def __getitem__(self, instruction: uuid.UUID) -> typing.Sequence[uuid.UUID]:
            return tuple(itertools.chain(reversed(self._prefixed[instruction]), self._absolute[instruction]))

        def insert(self, instruction: uuid.UUID, argument: uuid.UUID, index: typing.Optional[int] = None) -> None:
            """Store given argument as a positional parameter of given instruction at absolute offset given by index.

            Index can be omitted for single-argument instructions.

            Args:
                instruction: Target (receiver) instruction.
                argument: Positional argument to be stored.
                index: Position offset of given argument.
            """
            args = self._absolute[instruction]
            argcnt = len(args)
            if index is None:
                assert argcnt <= 1, f'Index required for multiarg ({argcnt}) instruction'
                index = 0
            assert index >= 0, 'Invalid positional index'
            if argcnt <= index:
                args.extend([None] * (index - argcnt + 1))
            assert not args[index], 'Link collision'
            args[index] = argument

        def update(self, node: grnode.Worker, getter: typing.Callable[[int], uuid.UUID]) -> None:
            """Register given node (its eventual functor) as an absolute positional argument of all of its subscribers.

            For multi-output nodes the output needs to be passed through Getter instructions that are extracting
            individual items.

            Args:
                node: Worker node (representing its actual functor) as an positional argument of its subscribers.
                getter: Callback for creating a Getter instruction for given positional index and returning its key.
            """
            if node.szout > 1:
                for index, output in enumerate(node.output):
                    source = getter(index)
                    self.insert(source, node.uid)
                    for subscriber in output:
                        self.insert(subscriber.node.uid, source, subscriber.port)
            else:
                for subscriber in node.output[0]:
                    self.insert(subscriber.node.uid, node.uid, subscriber.port)

        def prepend(self, instruction: uuid.UUID, argument: uuid.UUID) -> None:
            """In contrast to the absolute positional arguments we can potentially prepend these with various system
            arguments that should eventually prefix the absolute ones.

            Here we just append these to a list but during iteration we read them in reverse to reflect the prepend
            order.

            Args:
                instruction: Key of the target (receiver) instruction.
                argument: Argument (instruction key) to be prepended to the list of the absolute arguments.
            """
            self._prefixed[instruction].append(argument)

    class Index:
        """Mapping of the stored instructions.
        """
        def __init__(self):
            self._instructions: typing.Dict[uuid.UUID, assembly.Instruction] = dict()

        def __contains__(self, key: uuid.UUID) -> bool:
            return key in self._instructions

        def __getitem__(self, key: uuid.UUID):
            return self._instructions[key]

        def items(self) -> typing.ItemsView[uuid.UUID, assembly.Instruction]:
            """Content iterator.

            Returns: Tuples of key-instruction pairs.
            """
            return self._instructions.items()

        def set(self, instruction: assembly.Instruction, key: typing.Optional[uuid.UUID] = None) -> uuid.UUID:
            """Store given instruction by provided or generated key.

            It is an error to store instruction with existing key (to avoid, use the reset method).

            Args:
                instruction: Runtime instruction to be stored.
                key: Optional key to be used as instruction reference.

            Returns: Key associated with the instruction.
            """
            if not key:
                key = uuid.uuid4()
            assert key not in self, 'Instruction collision'
            self._instructions[key] = instruction
            return key

        def reset(self, orig: uuid.UUID, new: typing.Optional[uuid.UUID] = None) -> uuid.UUID:
            """Re-register instruction under given key to a new key (provided or generate).

            Args:
                orig: Original key of the instruction to be re-registered.
                new: Optional new key to re-register the instruction with.

            Returns: New key associated with the instruction.
            """
            instruction = self._instructions[orig]
            del self._instructions[orig]
            return self.set(instruction, new)

    def __init__(self, assets: access.State):
        self._assets: access.State = assets
        self._offsets: typing.Dict[uuid.UUID, int] = dict()
        self._linkage: Table.Linkage = self.Linkage()
        self._index: Table.Index = self.Index()
        self._committer: typing.Optional[uuid.UUID] = None

    def __iter__(self) -> assembly.Symbol:
        for key, instruction in self._index.items():
            yield assembly.Symbol(instruction, tuple(self._index[a] for a in self._linkage[key]))

    def add(self, node: grnode.Worker) -> None:
        """Populate the symbol table to implement the logical flow of given node.

        Args:
            node: Node to be added - compiled into symbols.
        """
        assert node.uid not in self._index, 'Node collision'

        LOGGER.debug('Adding node %s into the symbol table', node)
        functor = instmod.Mapper(node.spec)
        aliases = [node.uid]
        if node.stateful:
            state = node.gid
            offset = self._offsets.setdefault(state, len(self._offsets))
            if state not in self._index:
                self._index.set(instmod.Loader(self._assets, offset), state)
            if node.trained:
                functor = instmod.Consumer(node.spec)
                if not self._committer:
                    self._committer = self._index.set(instmod.Committer(self._assets))
                dumper = self._index.set(instmod.Dumper(self._assets))
                self._linkage.insert(dumper, node.uid)
                self._linkage.insert(self._committer, dumper, offset)
                aliases.append(state)
                state = self._index.reset(state)  # re-register loader under it's own id
            functor = functor.shiftby(instmod.Functor.Shifting.state)
            self._linkage.prepend(node.uid, state)
        for key in aliases:
            self._index.set(functor, key)

        self._linkage.update(node, lambda index: self._index.set(instmod.Getter(index)))

    def visit_node(self, node: grnode.Worker) -> None:
        """Visitor entrypoint.

        Args:
            node: Node to be visited.
        """
        self.add(node)


def generate(path: view.Path, assets: access.State) -> typing.Sequence[assembly.Symbol]:
    """Generate the symbol code based on given flow path.

    Args:
        path: Flow path to generate the symbols for.
        assets: Runtime assets dependencies.

    Returns: Sequence of symbol code.
    """
    table = Table(assets)
    path.accept(table)
    return tuple(table)
