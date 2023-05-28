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
ForML application model rollout strategy.
"""
import abc
import collections
import dataclasses
import functools
import logging
import threading
import time
import typing

from forml.io import asset as assetmod

if typing.TYPE_CHECKING:
    from forml import application, runtime
    from forml.io import asset  # pylint: disable=reimported


LOGGER = logging.getLogger(__name__)


class Selector(abc.ABC):
    """Abstract base class for the model selection strategy to be used by the
    :class:`application.Generic <forml.application.Generic>` descriptors.
    """

    @abc.abstractmethod
    def select(self, registry: 'asset.Directory', context: typing.Any, stats: 'runtime.Stats') -> 'asset.Instance':
        """Select the model instance to be used for serving the request.

        See Also:
            This serves the same purpose as the :meth:`application.Descriptor.select
            <forml.application.Descriptor.select>` method only extracted as a separate object.

        Args:
            registry: Model registry to select the model from.
            context: Optional metadata carried over from the :meth:`application.Descriptor.receive
                     <forml.application.Descriptor.receive>`.
            stats: Application specific serving metrics.

        Returns:
            Model instance.
        """


class Explicit(Selector):
    """Model selection strategy always choosing an explicit model generation.

    Args:
        project: Project reference of the selected model.
        release: Project release reference of the selected model.
        generation: Project generation reference of the selected model.
    """

    def __init__(
        self,
        project: typing.Union[str, 'asset.Project.Key'],
        release: typing.Union[str, 'asset.Release.Key'],
        generation: typing.Union[str, int, 'asset.Generation.Key'],
    ):
        self._project: typing.Union[str, 'asset.Project.Key'] = project
        self._release: typing.Union[str, 'asset.Release.Key'] = release
        self._generation: typing.Union[str, int, 'asset.Generation.Key'] = generation
        self._instance: typing.Optional['asset.Instance'] = None

    def select(self, registry: 'asset.Directory', context: typing.Any, stats: 'runtime.Stats') -> 'asset.Instance':
        if not self._instance:
            self._instance = assetmod.Instance(
                registry=registry,
                project=self._project,
                release=self._release,
                generation=self._generation,
            )
        return self._instance


class Latest(Selector):
    """Model selection strategy choosing an instance of the most recent model release/generation.

    Args:
        project: Project reference to choose the most recent generation from.
        release: Optional release to choose the most recent generation from.
        refresh: Interval in seconds for refreshing the latest instance from its registry.
    """

    DEFAULT_REFRESH = 30
    """Default refresh interval in seconds"""

    def __init__(
        self,
        project: typing.Union[str, 'asset.Project.Key'],
        release: typing.Optional[typing.Union[str, 'asset.Release.Key']] = None,
        refresh: float = DEFAULT_REFRESH,
    ):
        self._project: typing.Union[str, 'asset.Project.Key'] = project
        self._release: typing.Optional[typing.Union[str, 'asset.Release.Key']] = release
        self._interval: float = refresh
        self._cache: dict['asset.Directory', 'asset.Instance'] = {}
        self._lock: threading.RLock = threading.RLock()
        self._refresher: threading.Thread = threading.Thread(target=self._refresh, daemon=True)

    def __reduce__(self):
        return self.__class__, (self._project, self._release, self._interval)

    def _refresh(self) -> None:
        while True:
            with self._lock:
                instances = tuple(self._cache.items())
            LOGGER.debug('Refreshing %d cached instances', len(instances))
            for registry, old in instances:
                new = self._pick(registry)
                if new != old:
                    LOGGER.info('Updating latest instance to %s', new)
                    with self._lock:
                        self._cache[registry] = new
            time.sleep(self._interval)

    def _pick(self, registry: 'asset.Directory') -> 'asset.Instance':
        release = self._release
        generation = None
        if not release:
            project = registry.get(self._project)
            for release in reversed(project.list()):
                try:
                    generation = project.get(release).list().last
                except assetmod.Level.Listing.Empty:
                    continue
                break
            else:
                raise assetmod.Level.Listing.Empty(f'No models available for {self._project}')
        return assetmod.Instance(
            registry=registry,
            project=self._project,
            release=release,  # pylint: disable=undefined-loop-variable
            generation=generation,
        )

    def select(self, registry: 'asset.Directory', context: typing.Any, stats: 'runtime.Stats') -> 'asset.Instance':
        with self._lock:
            if registry not in self._cache:
                self._cache[registry] = self._pick(registry)
                if not self._refresher.is_alive():
                    self._refresher.start()
            return self._cache[registry]


class ABTest(Selector):
    """Model selection strategy choosing among a number of defined variants according to their
    target weights.

    The target weights can be defined either as fractions in the float interval (0, 1) or as
    positive integers that will get normalized internally. If omitted for any variant, the
    particular target weight gets calculated either as a complement to 1 (if all other provided
    weights are floats below 1) or as a mean of the provided integer weights.

    Attention:
        Instances are expected to be created via the :class:`Builder`.

    Examples:
        Setting up an ABTest starts with the :meth:`compare` method:

        >>> selector = application.ABTest.compare(
        >>>     "forml-tutorial-titanic", "0.1.dev1", 1, 0.9
        >>> ).against(generation=2, target=0.1)
    """

    class Variant(collections.namedtuple('Variant', 'project, release, generation, target')):
        """Internal container for holding the variant parameters."""

        project: typing.Union[str, 'asset.Project.Key']
        release: typing.Union[str, 'asset.Release.Key']
        generation: typing.Union[str, int, 'asset.Generation.Key']
        target: typing.Optional[float]

        def __new__(
            cls,
            project: typing.Union[str, 'asset.Project.Key'],
            release: typing.Union[str, 'asset.Release.Key'],
            generation: typing.Union[str, int, 'asset.Generation.Key'],
            target: typing.Optional[float],
        ):
            if target is not None and target <= 0:
                raise ValueError(f'Positive target required: {target}')
            return super().__new__(cls, project, release, generation, target)

        def __hash__(self):
            return hash(self.project) ^ hash(self.release) ^ hash(self.generation)

        def __eq__(self, other):
            return (
                isinstance(other, self.__class__)
                and other.project == self.project
                and other.release == self.release
                and other.generation == self.generation
            )

    class Builder:
        """Internal builder for setting up the :class:`application.ABTest
        <forml.application.ABTest>`.

        Attention:
            Instances are expected to be created via the :meth:`application.ABTest.compare
            <compare>` method.
        """

        def __init__(
            self,
            project: typing.Union[str, 'asset.Project.Key'],
            release: typing.Union[str, 'asset.Release.Key'],
            generation: typing.Union[str, int, 'asset.Generation.Key'],
            target: typing.Optional[float],
        ):
            self._variants: list['ABTest.Variant'] = [ABTest.Variant(project, release, generation, target)]

        def over(
            self,
            generation: typing.Union[str, int, 'asset.Generation.Key'],
            *,
            release: typing.Optional[typing.Union[str, 'asset.Release.Key']] = None,
            project: typing.Optional[typing.Union[str, 'asset.Project.Key']] = None,
            target: typing.Optional[float] = None,
        ) -> 'application.ABTest.Builder':
            """Intermediate method for adding a non-last variant (ABTest of more than 2 variants).

            Args:
                project: Project reference of the selected model.
                release: Project release reference of the selected model.
                generation: Project generation reference of the selected model.
                target: Relative engagement share demand.

            Returns:
                ABTest builder.
            """
            last = self._variants[-1]
            self._variants.append(ABTest.Variant(project or last.project, release or last.release, generation, target))
            return self

        def against(
            self,
            generation: typing.Union[str, int, 'asset.Generation.Key'],
            *,
            release: typing.Optional[typing.Union[str, 'asset.Release.Key']] = None,
            project: typing.Optional[typing.Union[str, 'asset.Project.Key']] = None,
            target: typing.Optional[float] = None,
        ) -> 'application.ABTest':
            """ABTest builder completer for adding the last variant and constructing the ABTest
            instance.

            Args:
                project: Project reference of the selected model.
                release: Project release reference of the selected model.
                generation: Project generation reference of the selected model.
                target: Relative engagement share demand.

            Returns:
                ABTest instance.
            """
            self.over(generation, release=release, project=project, target=target)
            return ABTest(*self._variants)  # pylint: disable=no-value-for-parameter

    @dataclasses.dataclass
    class Slot:
        """Internal container for variant metadata."""

        variant: 'application.ABTest.Variant'
        target: float
        count: int = 0

        def __hash__(self):
            return hash(self.variant)

        @functools.lru_cache
        def _instance(self, registry: 'asset.Directory') -> 'asset.Instance':
            return assetmod.Instance(
                registry=registry,
                project=self.variant.project,
                release=self.variant.release,
                generation=self.variant.generation,
            )

        def hit(self, registry: 'asset.Directory') -> 'asset.Instance':
            """Select this slot for serving.

            Args:
                registry: Model registry to select the model from.

            Returns:
                Model instance.
            """
            self.count += 1
            return self._instance(registry)

        def eligible(self, total: int) -> bool:
            """Check the eligibility of this slot given the provided total amount of requests.

            Args:
                total: Overall amount of requests served across all variants.
                       Must be > 0.
            Return:
                True if this slot is eligible for serving.
            """
            assert total > 0
            return (self.count / total) < self.target

    def __init__(
        self,
        avar: 'application.ABTest.Variant',
        bvar: 'application.ABTest.Variant',
        *others: 'application.ABTest.Variant',
    ):
        variants = (avar, bvar, *others)
        if len(set(variants)) != len(variants):
            raise ValueError('Exclusive variants required')
        targets = [v.target for v in variants if v.target]
        missing = sum(1 for v in variants if not v.target)
        if missing:
            explicit = sum(targets)
            implicit = (1 - explicit) / missing if explicit < 1 else explicit / len(targets)
            targets = [v.target or implicit for v in variants]
        combined = sum(targets)
        self._slots: tuple[ABTest.Slot] = tuple(
            sorted(
                (self.Slot(v, t / combined) for v, t in zip(variants, targets)), key=lambda s: s.target, reverse=True
            )
        )
        self._total: int = 0

    @classmethod
    def compare(
        cls,
        project: typing.Union[str, 'asset.Project.Key'],
        release: typing.Union[str, 'asset.Release.Key'],
        generation: typing.Union[str, int, 'asset.Generation.Key'],
        target: typing.Optional[float] = None,
    ) -> 'application.ABTest.Builder':
        """Bootstrap method for creating the ABTest builder.

        Args:
            project: Project reference of the selected model.
            release: Project release reference of the selected model.
            generation: Project generation reference of the selected model.
            target: Relative engagement share demand.

        Returns:
            ABTest builder.
        """
        return cls.Builder(project, release, generation, target)

    def select(self, registry: 'asset.Directory', context: typing.Any, stats: 'runtime.Stats') -> 'asset.Instance':
        self._total += 1
        for slot in self._slots:
            if slot.eligible(self._total):
                break
        else:
            raise RuntimeError('No eligible slots')
        return slot.hit(registry)
