import abc
import unittest

from forml.flow import task


class Stateless(unittest.TestCase):
    @abc.abstractmethod
    def actor(self) -> task.Actor:
        """
        Returns: User actor instance.
        """

    def setUp(self) -> None:
        self._actor: task.Actor = self.actor()

    def test_apply(self) -> None:
        self.assertEqual(_out, self._actor.apply(in_))