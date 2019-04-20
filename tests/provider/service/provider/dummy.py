"""
Dummy provider implementation.
"""
from tests.provider import service


class Provider(service.Provider, key='dummy'):
    """Provider implementation.
    """
    def serve(self) -> str:
        """No op.
        """
