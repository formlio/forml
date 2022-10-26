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
Special lightweight launcher for dummy execution.
"""
import logging
import types
import typing

from forml import io
from forml import project as prjmod
from forml import setup
from forml.io import asset
from forml.provider.registry.filesystem import volatile

from ..pipeline import payload
from . import _pad

if typing.TYPE_CHECKING:
    from forml import flow, project, runtime  # pylint: disable=reimported
    from forml.io import dsl

LOGGER = logging.getLogger(__name__)


class Virtual:
    """Custom launcher allowing to execute the provided artifact using the default or an
    explicit runner in combination with a special pipeline sink to capture and return any output
    produced by the given action.

    All states produced while executing the launcher actions are (temporarily) persisted in an
    internal model registry implemented by the :class:`Volatile provider
    <forml.provider.registry.filesystem.volatile.Registry>`.

    Args:
        artifact: Project artifact to be launched.

    The available launcher actions are exposed using the following common triggers:

    Methods:
        train(lower=None, upper=None) -> runtime.Virtual.Trained:
            Trigger the *train* action.

            Returns:
                Accessor of the train-mode features/outcomes outputs.

        tune(lower=None, upper=None) -> None:
            Trigger the *tune* action.

        apply(lower=None, upper=None) -> flow.Features:
            Trigger the *apply* action.

            Returns:
                The apply-mode output.

        eval(lower=None, upper=None) -> float:
            Trigger the (train-test) *eval* action.

            Returns:
                Evaluation metric value.

    Furthermore, these triggers can be accessed using three different approaches:

    #. Directly on the launcher instance:

       >>> launcher_instance.eval()
       0.83

    #. Using a *getitem* syntax specifying an explicit *runner* provider:

       >>> launcher_instance['graphviz'].train()

    #. Using a *call* syntax specifying both an explicit *runner* and a list of source *feeds*
       providers:

       >>> launcher_instance('dask', ['openlake']).apply()
       [0.31, 0.63, 0.16, 0.87]
    """

    class Trained:
        """Lazy accessor of the virtual train-mode features/outcomes segment outputs as returned by
        the :meth:`runtime.Virtual.train <forml.runtime.Virtual.train>` method."""

        def __init__(self, future: payload.Sniff.Value.Future):
            self._future: payload.Sniff.Value.Future = future

        @property
        def features(self) -> 'flow.Features':
            """Get the train-mode *features* segment values."""
            return self._future.result()[0]

        @property
        def labels(self) -> 'flow.Labels':
            """Get the train-mode *labels* segment values."""
            return self._future.result()[1]

    class Handler:
        """Wrapper for selected launcher parameters."""

        def __init__(self, launcher: _pad.Launcher, sniffer: payload.Sniff):
            self._launcher: _pad.Launcher = launcher
            self._sniffer: payload.Sniff = sniffer

        def _run(
            self,
            mode: 'runtime.Launcher.Mode',
            lower: typing.Optional['dsl.Native'] = None,
            upper: typing.Optional['dsl.Native'] = None,
        ) -> payload.Sniff.Value.Future:
            with self._sniffer as future:
                mode(lower, upper)
            return future

        def apply(
            self, lower: typing.Optional['dsl.Native'] = None, upper: typing.Optional['dsl.Native'] = None
        ) -> 'flow.Features':
            """Trigger the *apply* action.

            See Also: Full description in the Virtual class docstring.
            """
            future = self._run(self._launcher.apply, lower, upper)
            try:
                return future.result()
            except payload.Sniff.Lost as err:
                LOGGER.warning(err)
            return None

        def train(
            self, lower: typing.Optional['dsl.Native'] = None, upper: typing.Optional['dsl.Native'] = None
        ) -> 'runtime.Virtual.Trained':
            """Trigger the *train* action.

            See Also: Full description in the Virtual class docstring.
            """
            return Virtual.Trained(self._run(self._launcher.train_return, lower, upper))

        def eval(
            self, lower: typing.Optional['dsl.Native'] = None, upper: typing.Optional['dsl.Native'] = None
        ) -> float:
            """Trigger the *eval* action.

            See Also: Full description in the Virtual class docstring.
            """
            future = self._run(self._launcher.eval_traintest, lower, upper)
            try:
                return future.result()[0]
            except payload.Sniff.Lost as err:
                LOGGER.warning(err)
            return float('nan')

        @property
        def tune(self) -> typing.Callable[[typing.Optional['dsl.Native'], typing.Optional['dsl.Native']], None]:
            """Trigger the *tune* action.

            See Also: Full description in the Virtual class docstring.
            """
            return self._launcher.tune

    class Sink(io.Sink):
        """Sniffer sink."""

        def __init__(self, sniffer: payload.Sniff):
            super().__init__()
            self._sniffer: payload.Sniff = sniffer

        def save(self, schema: typing.Optional['dsl.Source.Schema']) -> 'flow.Composable':
            return self._sniffer

    def __init__(self, artifact: 'project.Artifact'):
        class Manifest(types.ModuleType):
            """Fake manifest module."""

            NAME = (artifact.package or setup.PRJNAME).replace('.', '-')
            VERSION = '0'
            PACKAGE = artifact.package
            MODULES = artifact.modules

            def __init__(self):
                super().__init__(prjmod.Manifest.MODULE)

        with setup.context(Manifest()):
            package = prjmod.Package(artifact.path or asset.mkdtemp(prefix='virtual-'))

        self._project: str = package.manifest.name
        self._registry: asset.Registry = volatile.Registry()
        self._sniffer: payload.Sniff = payload.Sniff()
        asset.Directory(self._registry).get(self._project).put(package)

    def __call__(
        self,
        runner: typing.Optional[typing.Union[setup.Runner, str]] = None,
        feeds: typing.Optional[typing.Iterable[typing.Union[setup.Feed, str, io.Feed]]] = None,
    ) -> 'runtime.Virtual.Handler':
        launcher = _pad.Platform(runner, self._registry, feeds, self.Sink(self._sniffer)).launcher(self._project)
        return self.Handler(launcher, self._sniffer)

    def __getitem__(self, runner: typing.Union[setup.Runner, str]) -> 'runtime.Virtual.Handler':
        """Convenient shortcut for selecting a specific runner using the `launcher[name]` syntax.

        Args:
            runner: Runner alias/qualname to use.

        Returns:
            Launcher handler.
        """
        return self(runner)

    def __getattr__(
        self, mode: str
    ) -> typing.Callable[[typing.Optional['dsl.Native'], typing.Optional['dsl.Native']], typing.Any]:
        """Convenient shortcut for accessing the particular launcher mode using the
        ``launcher.train()`` syntax.

        Args:
            mode: Launcher mode to execute.

        Returns:
            Callable launcher handler.
        """
        return getattr(self(), mode)
