"""
Service provider interface.
"""
import abc

from forml import provider


class Provider(provider.Interface):
    """Service interface.
    """
    @abc.abstractmethod
    def serve(self) -> str:
        """Just to make it abstract.
        """
