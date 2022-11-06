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

from . import _actor, _operator

if typing.TYPE_CHECKING:
    from forml import flow
    from forml.pipeline import wrap

LOGGER = logging.getLogger(__name__)


Entity = typing.TypeVar('Entity')
Class = typing.TypeVar('Class', bound=type)


class Auto(typing.Generic[Entity], abc.ABC):
    """Generic base class for auto-wrapper implementations.

    If supplied to the :func:`wrap.importer() <forml.pipeline.wrap.importer>` context manager
    when capturing the imports, each discovered entity within the imported namespace is checked
    against the auto-wrapper using its :meth:`match` method and if compatible it gets wrapped
    in-place using its :meth:`apply` method.

    Each auto-wrapper needs to implement the following methods:

    Methods:
        match(entity):
            Check this wrapper is capable of wrapping the given entity into a ForML operator.

            Args:
                entity: Wrapping candidate subject.

            Returns:
                True if this wrapper is capable to wrap the entity.

        apply(entity):
            Actual wrapping implementation.

            Args:
                entity: Wrapping subject.

            Returns:
                ForML `operator-type-like` callable compatible with the signature of the wrapped
                entity.
    """

    def __call__(self, entity: Entity) -> typing.Callable[..., 'flow.Operator']:
        if not self.match(entity):
            raise TypeError(f'Incompatible wrapping: {entity}')
        return self.apply(entity)

    def __repr__(self):
        return self.__class__.__name__

    @abc.abstractmethod
    def match(self, entity: typing.Any) -> bool:
        """Check this wrapper is capable of wrapping the given entity into a ForML operator.

        See Also: Full description in the class docstring.
        """

    @abc.abstractmethod
    def apply(self, entity: Entity) -> typing.Callable[..., 'flow.Operator']:
        """Actual wrapping implementation.

        See Also: Full description in the class docstring.
        """


class AutoClass(collections.namedtuple('AutoClass', 'base, apply'), typing.Generic[Class], Auto[Class]):
    """Class-specific wrapper."""

    base: Class
    apply: typing.Callable[[Class], typing.Callable[..., 'flow.Operator']]

    def __repr__(self):
        return Auto.__repr__(self)

    def match(self, entity: Class) -> bool:
        return inspect.isclass(entity) and issubclass(entity, self.base) and not inspect.isabstract(entity)


class AutoSklearnTransformer(AutoClass[type[sklbase.TransformerMixin]]):
    """Auto-wrapper for turning Scikit-learn *transformers* into ForML operators.

    Instances can be used with :func:`wrap.importer <forml.pipeline.wrap.importer>` to auto-wrap
    Scikit-learn transformers upon importing.

    Hint:
        Supports not just the official Scikit-learn transformers but any
        :class:`sklearn.base.TransformerMixin` subclasses including 3rd party implementations.

    Args:
        apply: Customizable :meth:`mapping <forml.pipeline.wrap.Actor.type>` for the *apply-mode*
               target endpoint. Defaults to a ``transform`` literal.
    """

    def __new__(cls, apply: typing.Union[str, typing.Callable[..., typing.Any]] = 'transform'):
        def wrap(transformer: type[sklbase.TransformerMixin]):
            return _operator.Operator.mapper(_actor.Actor.type(transformer, train='fit', apply=apply))

        return super().__new__(cls, sklbase.TransformerMixin, wrap)


class AutoSklearnClassifier(AutoClass[type[sklbase.ClassifierMixin]]):
    """AutoSklearnClassifier(apply: typing.Union[str, typing.Callable[..., typing.Any]] = predict_proba[-1])

    Auto-wrapper for turning Scikit-learn *classifiers* into ForML operators.

    Instances can be used with :func:`wrap.importer <forml.pipeline.wrap.importer>` to auto-wrap
    Scikit-learn classifiers upon importing.

    Hint:
        Supports not just the official Scikit-learn classifiers but any
        :class:`sklearn.base.ClassifierMixin` subclasses including 3rd party implementations.

    Args:
        apply: Customizable :meth:`mapping <forml.pipeline.wrap.Actor.type>` for the *apply-mode*
               target endpoint. Defaults to a callback hitting the ``.predict_proba`` and returning
               the last of its produced columns (conveniently the 1-class probability in case of
               binary classification; for multiclass this needs tweaking).
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


class AutoSklearnRegressor(AutoClass[type[sklbase.RegressorMixin]]):
    """Auto-wrapper for turning Scikit-learn *regressors* into ForML operators.

    Instances can be used with :func:`wrap.importer <forml.pipeline.wrap.importer>` to auto-wrap
    Scikit-learn regressors upon importing.

    Hint:
        Supports not just the official Scikit-learn regressors but any
        :class:`sklearn.base.RegressorMixin` subclasses including 3rd party implementations.

    Args:
        apply: Customizable :meth:`mapping <forml.pipeline.wrap.Actor.type>` for the *apply-mode*
               target endpoint. Defaults to a ``predict`` literal.
    """

    def __new__(cls, apply: typing.Union[str, typing.Callable[..., typing.Any]] = 'predict'):
        def wrap(regressor: type[sklbase.RegressorMixin]):
            return _operator.Operator.apply(_actor.Actor.type(regressor, train='fit', apply=apply))

        return super().__new__(cls, sklbase.RegressorMixin, wrap)


#: Default list of auto-wrapper implementations.
AUTO = [
    AutoSklearnTransformer(),
    AutoSklearnClassifier(),
    AutoSklearnRegressor(),
]


def _walk(owner: object, *attrs: str, seen: typing.Optional[set] = None) -> typing.Iterable[tuple[object, str, object]]:
    """Helper for traversing the given (or all) attributes of the owning object and returning all
    of its submodules or subclasses.

    Args:
        owner: Module or class to traverse.
        attrs: Potential explicit list of owner's attribute names to traverse.
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
        if (instance := getattr(owner, label, None)) is None:
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
    """Context manager capturing all direct imports and :ref:`wrapping <operator-autowrap>` their
    matching entities using the *explicit* or *default* list of *auto-wrappers*.

    The signature of the wrapped object is compatible with the original entity.

    Args:
        wrappers: Sequences of the auto-wrapper implementations to be matched and potentially
                  (if compatible) applied to the discovered wrapping candidates. If no explicit
                  value is provided, the default :attr:`wrap.AUTO <AUTO>` list of auto-wrapper
                  implementations is used.

    Returns:
        Context manager under which the direct imports become subject to auto-wrapping.

    Examples:
        All three possible import syntax alternatives are supported, although only the first
        one is recommended::

            with wrap.importer():
                # 1. auto-wrap just the explicit members (recommended):
                from sklearn.ensemble import GradientBoostingClassifier

                # 2. auto-wrap all members discovered in ensemble.*
                #    (not recommended - unnecessarily heavy)
                from sklearn import ensemble

                # 3. similar but without the namespace
                #    (even less recommended - heavy and dirty)
                from sklearn.ensemble import *

        Example use-case importing the ``sklearn.ensemble.GradientBoostingClassifier`` classifier
        wrapped as a ForML operator that can be directly used within a pipeline composition
        expression:

            >>> from forml import flow
            >>> from forml.pipeline import wrap
            >>>
            >>> with wrap.importer():
            ...     from sklearn.ensemble import RandomForestClassifier
            ...
            >>> RFC = RandomForestClassifier(n_estimators=30, max_depth=10)
            >>> isinstance(RFC, flow.Operator)
            True
            >>> PIPELINE = preprocessing.Prepare() >> RFC
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

        builtins.__import__ = native  # we don't want to be called for imports from imports...
        LOGGER.debug('Importing %s using autowrap', name)
        if not fromlist:  # might be None
            fromlist = ()
        _unload(name, *fromlist)  # drop cache to avoid patching existing instances and hitting any cached modules
        module = native(name, globals, locals, fromlist, level)
        patched = set()
        for owner, label, instance in _walk(module, *fromlist):
            for autowrap in wrappers:
                if autowrap.match(instance):
                    LOGGER.debug('Patching %s.%s using %s', owner.__name__, label, autowrap)
                    setattr(owner, label, autowrap(instance))
                    patched.add(owner.__name__)
                    break
        for owner in patched:  # drop cache so the patched instance doesn't get used elsewhere
            _unload(owner)
        builtins.__import__ = wrapping  # to again capture the next top-level import
        return module

    if not wrappers:
        wrappers = AUTO
    source = inspect.getmodule(inspect.currentframe().f_back.f_back)
    LOGGER.debug('Autowrap called from %s', source)
    native = builtins.__import__
    builtins.__import__ = wrapping
    yield
    builtins.__import__ = native
