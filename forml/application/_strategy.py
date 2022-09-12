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
import typing

from forml.io import asset as assetmod

if typing.TYPE_CHECKING:
    from forml import runtime
    from forml.io import asset  # pylint: disable=reimported


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

    Attention:
        Currently, the instance is cached indefinitely and so updates to the registry are not
        dynamically reflected.

    Args:
        project: Project reference to choose the most recent generation from.
        release: Optional release to choose the most recent generation from.
    """

    def __init__(
        self,
        project: typing.Union[str, 'asset.Project.Key'],
        release: typing.Optional[typing.Union[str, 'asset.Release.Key']] = None,
    ):
        self._project: typing.Union[str, 'asset.Project.Key'] = project
        self._release: typing.Optional[typing.Union[str, 'asset.Release.Key']] = release
        self._instance: typing.Optional['asset.Instance'] = None

    def select(self, registry: 'asset.Directory', context: typing.Any, stats: 'runtime.Stats') -> 'asset.Instance':
        if not self._instance:
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
            self._instance = assetmod.Instance(
                registry=registry,
                project=self._project,
                release=release,  # pylint: disable=undefined-loop-variable
                generation=generation,
            )
        return self._instance
