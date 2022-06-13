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
Auto wrapping features.
"""
import abc
import builtins
import collections
import contextlib
import inspect
import logging
import sys
import types
import typing

from sklearn import base as sklbase

from forml import flow

from . import _actor, _operator

if typing.TYPE_CHECKING:
    from forml.pipeline import wrap

LOGGER = logging.getLogger(__name__)


Subject = typing.TypeVar('Subject')
Class = typing.TypeVar('Class', bound=type)


class Auto(typing.Generic[Subject], abc.ABC):
    """Generic auto-wrapper base class."""

    def __call__(self, subject: Subject) -> typing.Callable[..., flow.Operator]:
        if not self.match(subject):
            raise TypeError(f'Incompatible wrapping: {subject}')
        return self.apply(subject)

    def __repr__(self):
        return self.__class__.__name__

    @abc.abstractmethod
    def match(self, subject: typing.Any) -> bool:
        """Check this wrapper is capable of wrapping the given subject into a ForML operator.

        Args:
            subject: Wrapping candidate subject.

        Returns:
            True if this wrapper is capable to wrap the subject.
        """

    @abc.abstractmethod
    def apply(self, subject: Subject) -> typing.Callable[..., flow.Operator]:
        """Actual wrapping implementation.

        Args:
            subject: Wrapping subject.

        Returns:
            ForML operator constructor-like callable with the same signature as the wrapped subject.
        """


class ClassWrapper(collections.namedtuple('ClassWrapper', 'base, apply'), typing.Generic[Class], Auto[Class]):
    """Class-specific wrapper."""

    base: Class
    apply: typing.Callable[[Class], typing.Callable[..., flow.Operator]]

    def __repr__(self):
        return Auto.__repr__(self)

    def match(self, subject: Class) -> bool:
        return inspect.isclass(subject) and issubclass(subject, self.base) and not inspect.isabstract(subject)


class SklearnTransformerWrapper(ClassWrapper[type[sklbase.TransformerMixin]]):
    """Wrapper for Scikit-learn transformers."""

    def __new__(cls, apply: typing.Union[str, typing.Callable[..., typing.Any]] = 'transform'):
        def wrap(transformer: type[sklbase.TransformerMixin]):
            return _operator.Operator.mapper(_actor.Actor.type(transformer, train='fit', apply=apply))

        return super().__new__(cls, sklbase.TransformerMixin, wrap)


class SklearnClassifierWrapper(ClassWrapper[type[sklbase.ClassifierMixin]]):
    """Wrapper for Scikit-learn classifiers.

    Apply mode (predict) mapper can be customized to choose between the typical `.predict_proba` or the actual class
    prediction using the `.predict`.
    """

    def __new__(
        cls,
        apply: typing.Union[str, typing.Callable[..., typing.Any]] = lambda c, *a, **kw: c.predict_proba(  # noqa: B008
            *a, **kw
        ).transpose()[-1],
    ):
        def wrap(classifier: type[sklbase.ClassifierMixin]):
            return _operator.Operator.apply(_actor.Actor.type(classifier, train='fit', apply=apply))

        return super().__new__(cls, sklbase.ClassifierMixin, wrap)


class SklearnRegressorWrapper(ClassWrapper[type[sklbase.RegressorMixin]]):
    """Wrapper for Scikit-learn regressors."""

    def __new__(cls, apply: typing.Union[str, typing.Callable[..., typing.Any]] = 'predict'):
        def wrap(regressor: type[sklbase.RegressorMixin]):
            return _operator.Operator.apply(_actor.Actor.type(regressor, train='fit', apply=apply))

        return super().__new__(cls, sklbase.RegressorMixin, wrap)


# Default set of wrappers.
WRAPPERS = SklearnTransformerWrapper(), SklearnClassifierWrapper(), SklearnRegressorWrapper()


def _walk(owner: object, *attrs: str, seen: typing.Optional[set] = None) -> typing.Iterable[tuple[object, str, object]]:
    """Helper for traversing the given (or all) attributes of the owning object and returning all of its submodules or
    subclasses.

    Args:
        owner: Module or class to traverse.
        attrs: Potential explicit list of base's attribute names to traverse.
        seen: Internal memo to avoid cycles.

    Returns:
        Iterator of tuple with owning object, attribute name and attribute instance.
    """
    if not seen:
        seen = set()
    if owner in seen:
        return
    seen.add(owner)
    for label in attrs or dir(owner):
        if not (instance := getattr(owner, label, None)):
            continue
        if inspect.ismodule(instance):  # dive into the module
            yield from _walk(instance, seen=seen)
            continue
        yield owner, label, instance
        if inspect.isclass(instance):  # also walk the inner classes
            yield from _walk(instance, seen=seen)


def _unload(name: str, *subs: str) -> None:
    """Helper for unloading the given module, all of its parents and potential submodules.

    Args:
        name: Module name to unload including all of its parents.
        subs: List of submodules to also unload.
    """
    for full in (f'{name}.{s}' for s in subs):
        if full in sys.modules:
            del sys.modules[full]
    while name in sys.modules:
        del sys.modules[name]
        name = name.rsplit('.', 1)[0]


@contextlib.contextmanager
def importer(*wrappers: 'wrap.Auto') -> typing.Iterable[None]:
    """Context manager capturing all direct imports and patching their matching attributes using the provided or default
    set of auto-wrappers.

    Args:
        wrappers: Sequences of the wrapper implementations to be applied to the matching attributes.

    Returns:
        Context manager.

    Examples:

        All the three possible import syntax alternatives are supported, although only the first one is recommended::

            with wrap.importer():
                # auto-wrap just the explicit members (recommended):
                from sklearn.ensemble import GradientBoostingClassifier

                # auto-wrap all members discovered in ensemble.* (not recommended - unnecessarily heavy)
                from sklearn import ensemble

                # similar but without the namespace (even less recommended - heavy and dirty)
                from sklearn.ensemble import *

        Example use-case importing the ``sklearn.ensemble.GradientBoostingClassifier`` classifier wrapped as
        a ForML operator that can be directly used within a pipeline composition expression::

            from forml import flow
            from forml.pipeline import wrap

            with wrap.importer():
                from sklearn.ensemble import GradientBoostingClassifier

            isinstance(GradientBoostingClassifier(), flow.Operator)  # True

            PIPELINE = someoperator() >> GradientBoostingClassifier(random_state=42)
    """

    def wrapping(
        name: str,
        globals=None,  # pylint: disable=redefined-builtin
        locals=None,  # pylint: disable=redefined-builtin
        fromlist=(),
        level=0,
    ) -> types.ModuleType:
        """Our injected importer function matching the original __import__ signature.

        Args:
            name: Module name to import.
            globals: Globals.
            locals: Locals.
            fromlist: Explicit list of components to be imported (and matched for wrapping).
            level:

        Returns:
            Imported module.
        """
        caller = inspect.getmodule(inspect.currentframe().f_back)
        if caller != source:  # ignore secondary imports
            return native(name, globals, locals, fromlist, level)

        LOGGER.debug('Importing %s using autowrap', name)
        if not fromlist:  # might be None
            fromlist = ()
        _unload(name, *fromlist)  # drop cache to avoid patching existing instances and hitting any cached modules
        module = native(name, globals, locals, fromlist, level)
        patched = set()
        for owner, label, instance in _walk(module, *fromlist):
            for wrap in wrappers:
                if wrap.match(instance):
                    LOGGER.debug('Patching %s.%s using %s', owner.__name__, label, wrap)
                    setattr(owner, label, wrap(instance))
                    patched.add(owner.__name__)
                    break
        for owner in patched:  # drop cache so the patched instance doesn't get used elsewhere
            _unload(owner)
        return module

    if not wrappers:
        wrappers = WRAPPERS
    source = inspect.getmodule(inspect.currentframe().f_back.f_back)
    LOGGER.debug('Autowrap called from %s', source)
    native = builtins.__import__
    builtins.__import__ = wrapping
    yield
    builtins.__import__ = native
