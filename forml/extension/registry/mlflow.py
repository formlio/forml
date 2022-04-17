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
Registry integration based on MLFlow Tracking Server.
"""
import functools
import logging
import os
import pathlib
import tempfile
import typing
import uuid

from mlflow import entities, tracking
from mlflow.projects import utils
from mlflow.store import entities as storent

import forml
from forml import project as prj
from forml.io import asset

LOGGER = logging.getLogger(__name__)

EntityT = typing.TypeVar('EntityT')


class Client:
    """Mlflow tracking server wrapper."""

    class Pager(typing.Generic[EntityT], typing.Iterable[EntityT]):
        """Rest API paging iterator."""

        def __init__(self, pager: typing.Callable[[typing.Optional[str]], storent.PagedList]):
            self._pager: typing.Callable[[typing.Optional[str]], storent.PagedList] = pager

        def __iter__(self) -> typing.Iterator[EntityT]:
            token = None
            while True:
                page = self._pager(token)
                yield from page
                if not page.token:
                    break
                token = page.token

    def __init__(self, *args, common_tags: typing.Optional[typing.Mapping[str, str]] = None, **kwargs):
        self._mlflow: tracking.MlflowClient = tracking.MlflowClient(*args, **kwargs)
        self._common_tags: typing.Mapping[str, str] = dict(common_tags) or {}

    def list_experiments(self) -> typing.Iterable[entities.Experiment]:
        """Get a list of the existing experiments.

        Returns:
            Iterator of Experiment instances.
        """
        return self.Pager[entities.Experiment](lambda t: self._mlflow.list_experiments(page_token=t))

    def list_runs(self, experiment: entities.Experiment, **tags: str) -> typing.Iterable[entities.Run]:
        """Get a list of the existing runs matching the given tags.

        Args:
            experiment: Project to list the runs under.
            tags: Tags to filter on.

        Returns:
            Iterator of Experiment instances.
        """
        return self.Pager[entities.Run](
            lambda t: self._mlflow.search_runs(
                [experiment.experiment_id],
                filter_string=' AND '.join(f"tag.{k} = '{v}'" for k, v in (self._common_tags | tags).items()),
                page_token=t,
            )
        )

    def download_artifact(self, run: entities.Run, name: str, dstdir: typing.Union[str, pathlib.Path]) -> pathlib.Path:
        """Fetch the file artifact stored under the run instance.

        Args:
            run: Run entity holding the artifact.
            name: Artifact name to fetch.
            dstdir: Target directory to download the artifact into.

        Returns:
            Local path of the downloaded artifact.
        """
        return pathlib.Path(dstdir) / self._mlflow.download_artifacts(run.info.run_id, name, str(dstdir))

    def upload_artifact(self, run: entities.Run, src: typing.Union[str, pathlib.Path]) -> None:
        """Store the file as an artifact under the given run instance.

        Args:
            run: Run entity to store the artifact under.
            src: Source path of the artifact.
        """
        self._mlflow.log_artifact(run.info.run_id, str(src))

    def set_tag(self, run: entities.Run, key: str, value: str) -> entities.Run:
        """Set a tag on the run instance.

        Args:
            run: Run entity to store the artifact under.
            key: Tag key.
            value: Tag value.

        Returns:
            Tagged run instance.
        """
        self._mlflow.set_tag(run.info.run_id, key, value)
        run.data.tags[key] = value
        return run

    @functools.cache
    def get_or_create_experiment(self, name: str) -> entities.Experiment:
        """Get an experiment instance by name if exists or create a new one.

        Args:
            name: Experiment name.

        Returns:
            Experiment entity instance.
        """
        entity = self._mlflow.get_experiment_by_name(name)
        if not entity:
            return self._mlflow.get_experiment(self._mlflow.create_experiment(name))
        if entity.lifecycle_stage != 'active':
            self._mlflow.restore_experiment(entity.experiment_id)
        return entity

    @functools.cache
    def get_or_create_run(self, experiment: str, **tags: str) -> entities.Run:
        """Get a run instance matching the given tags if exists or create a new one.

        Args:
            experiment: Experiment name.
            tags: Tags to filter on.

        Returns:
            Run entity instance.
        """
        eid = self.get_or_create_experiment(experiment).experiment_id
        tags = {k: str(v) for k, v in (self._common_tags | tags).items()}
        result = self._mlflow.search_runs(
            [eid],
            run_view_type=entities.ViewType.ALL,
            filter_string=' AND '.join(f"tag.{k} = '{v}'" for k, v in tags.items()),
        )
        if not result:
            return self._mlflow.create_run(eid, tags=tags)
        if len(result) == 1:
            run = result[0]
            if run.info.lifecycle_stage != 'active':
                self._mlflow.restore_run(run.info.run_id)
            return run
        raise forml.UnexpectedError(f'Multiple entities matching experiment `{experiment}` and tags {tags}')


class Registry(asset.Registry, alias='mlflow'):
    """ForML registry implementation backed by MLFlow tracking server."""

    TAG_RELEASE_KEY = utils.MLFLOW_GIT_COMMIT
    TAG_GENERATION_KEY = utils.MLFLOW_GIT_COMMIT
    TAG_REPOID = 'forml.repoid'
    """Tag for identifying repository object belonging to same virtual repository."""
    DEFAULT_REPOID = 'default'
    """Default virtual repository id."""
    TAG_SESSION = 'forml.session'
    """Identifier used for tagging unbounded generations."""
    TAG_RELEASE_REF = utils.MLFLOW_PARENT_RUN_ID
    TAG_LEVEL = 'forml.level'
    LEVEL_RELEASE = 'release'
    LEVEL_GENERATION = 'generation'
    STATESFX = 'bin'
    TAGFILE = 'tag.toml'
    PKGFILE = f'package.{prj.Package.FORMAT}'

    def __init__(
        self,
        tracking_uri: typing.Optional[str] = None,
        registry_uri: typing.Optional[str] = None,
        repoid: str = DEFAULT_REPOID,
        staging: typing.Optional[typing.Union[str, pathlib.Path]] = None,
    ):
        super().__init__(staging)
        self._client = Client(tracking_uri, registry_uri, common_tags={self.TAG_REPOID: repoid})
        self._session: uuid.UUID = uuid.uuid4()
        self._projects: dict[asset.Project.Key, entities.Experiment] = {}
        self._releases: dict[tuple[asset.Project.Key, asset.Release.Key], entities.Run] = {}
        self._generations: dict[tuple[asset.Project.Key, asset.Release.Key, asset.Generation.Key], entities.Run] = {}
        self._tmp: pathlib.Path = asset.mkdtemp()

    def projects(self) -> typing.Iterable[typing.Union[str, asset.Project.Key]]:
        for experiment in self._client.list_experiments():
            key = asset.Project.Key(experiment.name)
            self._projects[key] = experiment
            yield key

    def releases(self, project: asset.Project.Key) -> typing.Iterable[typing.Union[str, asset.Release.Key]]:
        for run in self._client.list_runs(self._projects[project], **{self.TAG_LEVEL: self.LEVEL_RELEASE}):
            key = asset.Release.Key(run.data.tags[self.TAG_RELEASE_KEY])
            self._releases[project, key] = run
            yield key

    def generations(
        self, project: asset.Project.Key, release: asset.Release.Key
    ) -> typing.Iterable[typing.Union[str, int, asset.Generation.Key]]:
        for run in self._client.list_runs(
            self._projects[project],
            **{
                self.TAG_RELEASE_REF: self._releases[project, release].info.run_id,
                self.TAG_LEVEL: self.LEVEL_GENERATION,
            },
        ):
            if self.TAG_GENERATION_KEY not in run.data.tags:
                continue  # unbounded generation
            key = asset.Generation.Key(run.data.tags[self.TAG_GENERATION_KEY])
            self._generations[project, release, key] = run
            yield key

    def pull(self, project: asset.Project.Key, release: asset.Release.Key) -> prj.Package:
        return prj.Package(
            self._client.download_artifact(
                self._releases[project, release], self.PKGFILE, tempfile.mkdtemp(dir=self._tmp)
            )
        )

    def push(self, package: prj.Package) -> None:
        assert package.path.is_file(), 'Expecting file package'
        project = package.manifest.name
        release = package.manifest.version
        with tempfile.TemporaryDirectory(dir=self._tmp) as tmp:
            path = pathlib.Path(tmp) / self.PKGFILE
            path.write_bytes(package.path.read_bytes())
            self._client.upload_artifact(self._get_release(project, release), path)

    def read(
        self, project: asset.Project.Key, release: asset.Release.Key, generation: asset.Generation.Key, sid: uuid.UUID
    ) -> bytes:
        with tempfile.TemporaryDirectory(dir=self._tmp) as tmp:
            try:
                artifact = self._client.download_artifact(
                    self._generations[project, release, generation], f'{sid}.{self.STATESFX}', tmp
                )
            except FileNotFoundError:
                LOGGER.warning('No state %s under runid %s', sid, self._generations[project, release, generation])
                return bytes()
            with artifact.open('rb') as statefile:
                return statefile.read()

    def write(self, project: asset.Project.Key, release: asset.Release.Key, sid: uuid.UUID, state: bytes) -> None:
        unbounded_generation = self._get_unbound_generation(project, release)
        with tempfile.TemporaryDirectory(dir=self._tmp) as tmp:
            path = os.path.join(tmp, f'{sid}.{self.STATESFX}')
            with open(path, 'wb') as statefile:
                statefile.write(state)
            self._client.upload_artifact(unbounded_generation, path)

    def open(
        self, project: asset.Project.Key, release: asset.Release.Key, generation: asset.Generation.Key
    ) -> asset.Tag:
        with tempfile.TemporaryDirectory(dir=self._tmp) as tmp:
            try:
                artifact = self._client.download_artifact(
                    self._generations[project, release, generation], self.TAGFILE, tmp
                )
            except FileNotFoundError as err:
                raise asset.Level.Listing.Empty(
                    f'No tag under runid {self._generations[project, release, generation]}'
                ) from err
            with artifact.open('rb') as tagfile:
                return asset.Tag.loads(tagfile.read())

    def close(
        self, project: asset.Project.Key, release: asset.Release.Key, generation: asset.Generation.Key, tag: asset.Tag
    ) -> None:
        unbounded_generation = self._get_unbound_generation(project, release)
        with tempfile.TemporaryDirectory(dir=self._tmp) as tmp:
            path = os.path.join(tmp, self.TAGFILE)
            with open(path, 'wb') as tagfile:
                tagfile.write(tag.dumps())
            self._client.upload_artifact(unbounded_generation, path)
        self._generations[project, release, generation] = self._client.set_tag(
            unbounded_generation, self.TAG_GENERATION_KEY, f'{generation}'
        )

    def _get_release(self, project: asset.Project.Key, release: asset.Release.Key) -> entities.Run:
        """Get the run instance for the release if exists or create a new one.

        Args:
            project: Project key.
            release: Release key.

        Returns:
            Run entity instance.
        """
        if (project, release) not in self._releases:
            self._releases[project, release] = self._client.get_or_create_run(
                project, **{self.TAG_LEVEL: self.LEVEL_RELEASE, self.TAG_RELEASE_KEY: release}
            )
        return self._releases[project, release]

    def _get_unbound_generation(self, project: asset.Project.Key, release: asset.Release.Key) -> entities.Run:
        """Get the run instance for unbounded generation under the given release if exists or create a new one.

        Args:
            project: Project key.
            release: Release key.

        Returns:
            Run entity instance.
        """
        return self._client.get_or_create_run(
            project,
            **{
                self.TAG_LEVEL: self.LEVEL_GENERATION,
                self.TAG_RELEASE_REF: self._get_release(project, release).info.run_id,
                self.TAG_SESSION: self._session,
            },
        )
