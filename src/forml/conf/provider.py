"""
ForML provider configs.
"""
import abc
import types

import typing

from forml import conf
from forml.conf import section as secmod


class Section(metaclass=secmod.Meta):
    """Provider config container.
    """
    PATTERN = r'\s*(\w+)\s*$'
    FIELDS = 'name, kwargs'
    SELECTOR: str = conf.SECTION_DEFAULT
    SUBJECT: str = abc.abstractmethod

    def __new__(cls, ref: str):
        section = f'{cls.SUBJECT.upper()}:{ref}'  # pylint: disable=no-member
        secmod.ensure(conf.PARSER, section)
        kwargs = dict()
        for option, value in conf.PARSER.items(section):
            if conf.PARSER.remove_option(section, option):  # take only non-default options
                conf.PARSER.set(section, option, value)
                kwargs[option] = value
        provider = kwargs.pop(conf.OPT_PROVIDER, ref)
        return super().__new__(cls, provider, types.MappingProxyType(kwargs))

    @classmethod
    def _default(cls) -> 'Section':
        """Default parsing.
        """
        return cls.parse(conf.get(cls.SUBJECT, cls.SELECTOR))

    # pylint: disable=no-member
    @classmethod
    def parse(cls, ref: typing.Optional[str] = None) -> 'Section':
        """Resolve the provider config.

        Args:
            ref: Provider alias.

        Returns: Provider config instance.
        """
        return super().parse(ref)[0] if ref else cls.default

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.name == self.name


class Registry(Section):
    """Registry provider.
    """
    SUBJECT = conf.OPT_REGISTRY


class Feed(Section):
    """Feed provider.
    """
    SUBJECT = conf.OPT_FEED


class Runner(Section):
    """Runner provider.
    """
    SUBJECT = conf.OPT_RUNNER


class Testing:
    """Providers specific to testing.
    """
    class Runner(Section):
        """Runner provider.
        """
        SUBJECT = conf.OPT_RUNNER
        SELECTOR = conf.SECTION_TESTING
