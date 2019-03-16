"""Project setup mechanics.
"""
import collections
import importlib
import sys
import typing

from forml import etl
from forml.flow import segment

import setuptools

from forml.project import component as importer


def setup(**kwargs) -> setuptools.dist.Distribution:
    """Setuptools wrapper for defining user projects using setup.py.

    Args:
        **kwargs:

    Returns:

    """
    return setuptools.setup(**kwargs)


class Descriptor(collections.namedtuple('Descriptor', 'pipeline, source')):
    """Top level ForML project descriptor holding the implementations of individual project components.
    """
    class Builder(collections.abc.Set):
        """Descriptor builder allowing to setup attributes one by one.
        """
        class Handler:
            """Simple callable that persists the provided value.
            """
            def __init__(self):
                self.value = None

            def __call__(self, value: typing.Any) -> None:
                self.value = value

            def __bool__(self):
                return self.value is not None

        def __init__(self):
            self._handlers: typing.Mapping[str, Descriptor.Builder.Handler] = {
                c:  self.Handler() for c in Descriptor._fields}

        def __iter__(self) -> typing.Iterator[typing.Tuple[str, typing.Callable[[typing.Any], None]]]:
            for component, handler in self._handlers.items():
                yield component, handler

        def __len__(self):
            return len(self._handlers)

        def __contains__(self, item):
            return item in self._handlers.keys()

        def build(self) -> 'Descriptor':
            """Create the descriptor.

            Returns: Descriptor instance.
            """
            if not all(self._handlers.values()):
                raise ValueError(f'Incomplete builder (missing {", ".join(c for c, h in self if not h)})')
            return Descriptor(*(self._handlers[c].value for c in Descriptor._fields))

    def __new__(cls, pipeline: segment.Composable, source: etl.Source):
        if not isinstance(pipeline, segment.Composable):
            raise ValueError('Invalid pipeline')
        if not isinstance(source, etl.Source):
            raise ValueError('Invalid source')
        return super().__new__(cls, pipeline, source)

    @classmethod
    def load(cls, package: typing.Optional[str] = None, **modkw) -> 'Descriptor':
        """Setup the descriptor based on provider package and/or individual modules.

            Either package is provided and all individual modules are considered as relative to that package or each
            module must be specified absolutely.

        Args:
            package: Base package to be considered as a root for all component modules.
            **modkw: Component module mappings.
        Returns: Project descriptor.
        """
        builder = cls.Builder()
        if not modkw.keys() <= builder:
            raise ValueError('Unexpected keyword argument')
        package = f'{package.rstrip(".")}.' if package else ''
        for component, handler in builder:
            module = package + (modkw.get(component) or component)
            with importer.Context(handler):
                if module in sys.modules:
                    if sys.modules[module].__package__:
                        del sys.modules[sys.modules[module].__package__]
                    del sys.modules[module]
                importlib.import_module(module)
        return builder.build()
