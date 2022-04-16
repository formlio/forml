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

from forml.io import asset, layout


class Selector(abc.ABC):
    """Model selection strategy base class."""

    @abc.abstractmethod
    def __call__(self, registry: asset.Directory, scope: typing.Any, stats: layout.Stats) -> asset.Instance:
        """Select the model instance to be used for serving the request.

        Args:
            registry: Model registry to select the model from.
            scope: Optional metadata carried over from decode.
            stats: Application specific serving metrics.

        Returns:
            Model instance.
        """


class Explicit(Selector):
    """Select an explicit generation."""

    def __init__(
        self,
        project: typing.Union[str, asset.Project.Key],
        release: typing.Union[str, asset.Release.Key],
        generation: typing.Union[str, int, asset.Generation.Key],
    ):
        self._project: typing.Union[str, asset.Project.Key] = project
        self._release: typing.Union[str, asset.Release.Key] = release
        self._generation: typing.Union[str, int, asset.Generation.Key] = generation
        self._instance: typing.Optional[asset.Instance] = None

    def __call__(self, registry: asset.Directory, scope: typing.Any, stats: layout.Stats) -> asset.Instance:
        if not self._instance:
            self._instance = asset.Instance(
                registry=registry,
                project=self._project,
                release=self._release,
                generation=self._generation,
            )
        return self._instance


class Latest(Selector):
    """Select an instance of the most recent model release/generation.

    Currently, the instance is cached indefinitely and so updates to the registry are not dynamically reflected.
    """

    def __init__(
        self,
        project: typing.Union[str, asset.Project.Key],
        release: typing.Optional[typing.Union[str, asset.Release.Key]] = None,
    ):
        self._project: typing.Union[str, asset.Project.Key] = project
        self._release: typing.Optional[typing.Union[str, asset.Release.Key]] = release
        self._instance: typing.Optional[asset.Instance] = None

    def __call__(self, registry: asset.Directory, scope: typing.Any, stats: layout.Stats) -> asset.Instance:
        if not self._instance:
            release = self._release
            generation = None
            if not release:
                project = registry.get(self._project)
                for release in reversed(project.list()):
                    try:
                        generation = project.get(release).list().last
                    except asset.Level.Listing.Empty:
                        continue
                    break
                else:
                    raise asset.Level.Listing.Empty(f'No models available for {self._project}')
            self._instance = asset.Instance(
                registry=registry,
                project=self._project,
                release=release,  # pylint: disable=undefined-loop-variable
                generation=generation,
            )
        return self._instance
