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
Main command.
"""
# pylint: disable=no-self-argument, no-self-use
import typing

import forml
from forml.conf.parsed import provider as provcfg
from forml.io import dsl
from forml.runtime import facility

from . import _api


class Forml(_api.Parser, description='Lifecycle Management for Datascience Projects'):
    """ForML command parser."""

    @_api.Command(help='create skeleton for a new project', description='New project setup')
    @_api.Param('name', help='name of a project to be created')
    def init(cls, name: str) -> None:
        """New project setup.

        Args:
            name: Project name to create.
        """
        raise forml.MissingError(f'Creating project {name}... not implemented')

    @_api.Command(help='show the content of the selected registry', description='Persistent registry listing')
    @_api.Param('project', nargs='?', help='project to be listed')
    @_api.Param('lineage', nargs='?', help='lineage to be listed')
    @_api.Param('-P', '--registry', type=str, help='persistent registry reference')
    def list(cls, project: typing.Optional[str], lineage: typing.Optional[str], registry: typing.Optional[str]) -> None:
        """Repository listing subcommand.

        Args:
            project: Name of project to be listed.
            lineage: Lineage version to be listed.
            registry: Optional registry reference.
        """
        _api.lprint(cls._platform(registry=registry).registry.list(project, lineage))

    @classmethod
    def _platform(
        cls,
        runner: typing.Optional[str] = None,
        registry: typing.Optional[str] = None,
        feed: typing.Optional[typing.Sequence[str]] = None,
        sink: typing.Optional[str] = None,
    ) -> facility.Platform:
        """Common helper for train/apply methods.

        Args:
            runner: Optional runner reference.
            registry: Optional registry reference.
            feed: Optional feed references.
            sink: Optional sink reference.

        Returns:
            Platform instance.
        """
        return facility.Platform(
            provcfg.Runner.resolve(runner),
            provcfg.Registry.resolve(registry),
            provcfg.Feed.resolve(feed),
            provcfg.Sink.Mode.resolve(sink),
        )

    @_api.Command(help='tune new generation of given (or default) project lineage', description='Tune mode execution')
    @_api.Param('project', help='project to be tuned')
    @_api.Param('lineage', nargs='?', help='lineage to be tuned')
    @_api.Param('generation', nargs='?', help='generation to be tuned')
    @_api.Param('-R', '--runner', type=str, help='runtime runner reference')
    @_api.Param('-P', '--registry', type=str, help='persistent registry reference')
    @_api.Param('-I', '--feed', nargs='*', type=str, help='input feed references')
    @_api.Param('--lower', help='lower tuneset ordinal')
    @_api.Param('--upper', help='upper tuneset ordinal')
    def tune(
        cls,
        project: typing.Optional[str],
        lineage: typing.Optional[str],
        generation: typing.Optional[str],
        runner: typing.Optional[str],
        registry: typing.Optional[str],
        feed: typing.Optional[typing.Sequence[str]],
        lower: typing.Optional[dsl.Native],
        upper: typing.Optional[dsl.Native],
    ) -> None:
        """Tune mode execution.

        Args:
            project: Name of project to be tuned.
            lineage: Lineage version to be tuned.
            generation: Generation index to be tuned.
            runner: Optional runner reference.
            registry: Optional registry reference.
            feed: Optional feed references.
            lower: Lower ordinal.
            upper: Upper ordinal.
        """
        raise forml.MissingError(f'Tuning project {project}... not implemented')

    @_api.Command(help='train new generation of given (or default) project lineage', description='Train mode execution')
    @_api.Param('project', help='project to be trained')
    @_api.Param('lineage', nargs='?', help='lineage to be trained')
    @_api.Param('generation', nargs='?', help='generation to be trained')
    @_api.Param('-R', '--runner', type=str, help='runtime runner reference')
    @_api.Param('-P', '--registry', type=str, help='persistent registry reference')
    @_api.Param('-I', '--feed', nargs='*', type=str, help='input feed references')
    @_api.Param('--lower', help='lower trainset ordinal')
    @_api.Param('--upper', help='upper trainset ordinal')
    def train(
        cls,
        project: typing.Optional[str],
        lineage: typing.Optional[str],
        generation: typing.Optional[str],
        runner: typing.Optional[str],
        registry: typing.Optional[str],
        feed: typing.Optional[typing.Sequence[str]],
        lower: typing.Optional[dsl.Native],
        upper: typing.Optional[dsl.Native],
    ) -> None:
        """Train mode execution.

        Args:
            project: Name of project to be tuned.
            lineage: Lineage version to be tuned.
            generation: Generation index to be tuned.
            runner: Optional runner reference.
            registry: Optional registry reference.
            feed: Optional feed references.
            lower: Lower ordinal.
            upper: Upper ordinal.
        """
        cls._platform(runner, registry, feed).launcher(project, lineage, generation).train(lower, upper)

    @_api.Command(help='apply given (or default) generation', description='Apply mode execution')
    @_api.Param('project', help='project to be applied')
    @_api.Param('lineage', nargs='?', help='lineage to be applied')
    @_api.Param('generation', nargs='?', help='generation to be applied')
    @_api.Param('-R', '--runner', type=str, help='runtime runner reference')
    @_api.Param('-P', '--registry', type=str, help='persistent registry reference')
    @_api.Param('-I', '--feed', nargs='*', type=str, help='input feed references')
    @_api.Param('-O', '--sink', type=str, help='output sink reference')
    @_api.Param('--lower', help='lower testset ordinal')
    @_api.Param('--upper', help='upper testset ordinal')
    def apply(
        cls,
        project: typing.Optional[str],
        lineage: typing.Optional[str],
        generation: typing.Optional[str],
        runner: typing.Optional[str],
        registry: typing.Optional[str],
        feed: typing.Optional[typing.Sequence[str]],
        sink: typing.Optional[str],
        lower: typing.Optional[dsl.Native],
        upper: typing.Optional[dsl.Native],
    ) -> None:
        """Apply mode execution.

        Args:
            project: Name of project to be tuned.
            lineage: Lineage version to be tuned.
            generation: Generation index to be tuned.
            runner: Optional runner reference.
            registry: Optional registry reference.
            feed: Optional feed references.
            sink: Optional sink reference.
            lower: Lower ordinal.
            upper: Upper ordinal.
        """
        cls._platform(runner, registry, feed, sink).launcher(project, lineage, generation).apply(lower, upper)

    @_api.Command(help='evaluate predictions of given (or default) generation', description='Eval mode execution')
    @_api.Param('project', help='project to be applied')
    @_api.Param('lineage', nargs='?', help='lineage to be applied')
    @_api.Param('generation', nargs='?', help='generation to be applied')
    @_api.Param('-R', '--runner', type=str, help='runtime runner reference')
    @_api.Param('-P', '--registry', type=str, help='persistent registry reference')
    @_api.Param('-I', '--feed', nargs='*', type=str, help='input feed references')
    @_api.Param('-O', '--sink', type=str, help='output sink reference')
    @_api.Param('--lower', help='lower testset ordinal')
    @_api.Param('--upper', help='upper testset ordinal')
    def eval(
        cls,
        project: typing.Optional[str],
        lineage: typing.Optional[str],
        generation: typing.Optional[str],
        runner: typing.Optional[str],
        registry: typing.Optional[str],
        feed: typing.Optional[typing.Sequence[str]],
        sink: typing.Optional[str],
        lower: typing.Optional[dsl.Native],
        upper: typing.Optional[dsl.Native],
    ) -> None:
        """Eval mode execution.

        Args:
            project: Name of project to be evaluated.
            lineage: Lineage version to be evaluated.
            generation: Generation index to be evaluated.
            runner: Optional runner reference.
            registry: Optional registry reference.
            feed: Optional feed references.
            sink: Optional sink reference.
            lower: Lower ordinal.
            upper: Upper ordinal.
        """
        cls._platform(runner, registry, feed, sink).launcher(project, lineage, generation).apply_eval(lower, upper)
