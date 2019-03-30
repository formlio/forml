"""
Runtime assembly symbols.
"""

import collections
import itertools
import typing
import uuid

from forml.flow.graph import node as grnode
from forml.runtime.assembly import instruction as instmod
from forml.runtime import assembly


class Table:
    """Dynamic builder of the runtime symbols. Table uses node UIDs and GIDs where possible as instruction keys.
    """
    class Index:
        """Structure for registering instruction dependency tree as relations between target (receiving) instruction
        and its upstream dependency instructions representing its positional arguments.
        """
        def __init__(self):
            self._absolute: typing.Dict[uuid.UUID,
                                        typing.List[typing.Optional[uuid.UUID]]] = collections.defaultdict(list)
            self._prefixed: typing.Dict[uuid.UUID,
                                        typing.List[typing.Optional[uuid.UUID]]] = collections.defaultdict(list)

        def items(self) -> typing.Iterable[typing.Tuple[uuid.UUID, typing.Sequence[uuid.UUID]]]:
            """Iterator of the final relations between target (receiver) instruction and its positional arguments.

            Returns: Iterator of tuples representing instructions and their positional arguments
            """
            for instruction, arguments in ((i, itertools.chain(reversed(self._prefixed.get(i, [])),
                                                               self._absolute.get(i, [])))
                                           for i in self._prefixed.keys() | self._absolute.keys()):
                yield instruction, tuple(arguments)

        def insert(self, instruction: uuid.UUID, argument: uuid.UUID, index: typing.Optional[int] = None) -> None:
            """Store given argument as a positional parameter of given instruction at absolute offset given by index.

            Index can be ommitted for single-argument instructions.

            Args:
                instruction: Target (receiver) instruction.
                argument: Positional argument to be stored.
                index: Position offset of given argument.
            """
            args = self._absolute[instruction]
            argsz = len(args)
            if index is None:
                assert argsz == 1, 'Index required for multiarg instruction'
                index = 0
            assert index >= 0, 'Invalid positional index'
            if argsz <= index:
                args.extend([None] * (index - argsz + 1))
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

    class Content:
        """Mapping of the stored instructions.
        """
        def __init__(self):
            self._instructions: typing.Dict[uuid.UUID, assembly.Instruction] = dict()

        def __contains__(self, key: uuid.UUID) -> bool:
            return key in self._instructions

        def __getitem__(self, key: uuid.UUID):
            return self._instructions[key]

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

    def __init__(self):
        self._index: Table.Index = self.Index()
        self._content: Table.Content = self.Content()
        self._committer: typing.Optional[uuid.UUID] = None

    def __iter__(self) -> assembly.Symbol:
        for instruction, arguments in self._index.items():
            yield assembly.Symbol(self._content[instruction], tuple(self._content[a] for a in arguments))

    def add(self, node: grnode.Worker) -> None:
        """Populate the symbol table to implement the logical flow of given node.

        Args:
            node: Node to be added - compiled into symbols.
        """
        assert node.uid not in self._content, 'Node collision'

        functor = instmod.Mapper(node.spec)
        aliases = [node.uid]
        if node.stateful:
            state = node.gid
            if state not in self._content:
                self._content.set(instmod.Loader(..., state), state)
            if node.trained:
                functor = instmod.Consumer(node.spec)
                if not self._committer:
                    self._committer = self._content.set(instmod.Committer(committer_args))
                dumper = self._content.set(instmod.Dumper(dumper_args))
                self._index.insert(dumper, node.uid)
                self._index.insert(self._committer, dumper, state_offset)
                aliases.append(state)
                state = self._content.reset(state)  # re-register loader under it's own id
            functor = functor.shiftby(instmod.Functor.Shifting.state)
            self._index.prepend(node.uid, state)
        for key in aliases:
            self._content.set(functor, key)

        self._index.update(node, lambda index: self._content.set(instmod.Getter(index)))
