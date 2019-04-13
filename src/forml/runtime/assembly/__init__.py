"""
ForML runtime task graph assembly logic.
"""
import abc
import collections
import logging
import time
import typing

LOGGER = logging.getLogger(__name__)


class Instruction(metaclass=abc.ABCMeta):
    """Callable part of an assembly symbol that's responsible for implementing the processing activity.
    """
    @abc.abstractmethod
    def execute(self, *args: typing.Any) -> typing.Any:
        """Instruction functionality.

        Args:
            *args: Sequence of input arguments.

        Returns: Instruction result.
        """

    def __str__(self):
        return self.__class__.__name__

    def __call__(self, *args: typing.Any) -> typing.Any:
        LOGGER.debug('%s invoked (%d args)', self, len(args))
        start = time.time()
        result = self.execute(*args)
        LOGGER.debug('%s completed (%.2fms)', self, (time.time() - start) * 1000)
        return result


class Symbol(collections.namedtuple('Symbol', 'instruction, arguments')):
    """Main entity of the assembled code.
    """
    def __new__(cls, instruction: Instruction, arguments: typing.Optional[typing.Sequence[Instruction]] = None):
        if arguments is None:
            arguments = []
        assert all(arguments), 'All arguments required'
        return super().__new__(cls, instruction, tuple(arguments))

    def __str__(self):
        return f'{self.instruction}{self.arguments}'
