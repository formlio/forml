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
import multiprocessing
import queue as quemod
import typing

from forml import io
from forml.conf.parsed import provider as provcfg
from forml.io import asset, dsl, layout
from forml.provider.registry.filesystem import volatile

from . import _pad

if typing.TYPE_CHECKING:
    from forml import project, runtime

LOGGER = logging.getLogger(__name__)


class Virtual:
    """Custom launcher allowing to execute the provided release package using the default or an
    explicit runner in combination with a special pipeline sink to capture and return any output
    produced by the given action.

    All states produced while executing the launcher actions are (temporarily) persisted in an
    internal model registry implemented by the :class:`Volatile provider
    <forml.provider.registry.filesystem.volatile.Registry>`.

    Args:
        package: Project release package to be launched.

    The available launcher actions are exposed using the following common triggers:

    Methods:
        train(lower=None, upper=None) -> None: Trigger the *train* action.
        tune(lower=None, upper=None) -> None: Trigger the *tune* action.
        apply(lower=None, upper=None) -> typing.Any: Trigger the *apply* action.
        eval(lower=None, upper=None) -> float: Trigger the (train-test) *eval* action.

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

    class Handler:
        """Wrapper for selected launcher parameters."""

        class Return:
            """Advanced action calling the given mode with injected Sink instance that captures the
            output using a multiprocessing queue and returns that output on exit."""

            class Sink(io.Sink):
                """Special sink to forward the output to a multiprocessing.Queue."""

                class Writer:
                    """Sink writer.

                    This should implement io.Consumer, but we don't really need it to return
                    anything.
                    """

                    def __init__(self, _: typing.Optional[dsl.Source.Schema], queue: multiprocessing.Queue):
                        self._queue: multiprocessing.Queue = queue

                    def __call__(self, data: layout.RowMajor) -> None:
                        self._queue.put(data, block=False)

            def __init__(
                self,
                handler: typing.Callable[[typing.Optional[io.Sink]], 'runtime.Launcher'],
                action: typing.Callable[
                    ['runtime.Launcher'],
                    typing.Callable[[typing.Optional[layout.Native], typing.Optional[layout.Native]], None],
                ],
            ):
                self._handler: typing.Callable[[typing.Optional[io.Sink]], 'runtime.Launcher'] = handler
                self._action: typing.Callable[
                    ['runtime.Launcher'],
                    typing.Callable[[typing.Optional[layout.Native], typing.Optional[layout.Native]], None],
                ] = action

            def __call__(
                self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None
            ) -> typing.Any:
                with multiprocessing.Manager() as manager:
                    output = manager.Queue()
                    self._action(self._handler(self.Sink(queue=output)))(lower, upper)
                    try:
                        return output.get_nowait()
                    except quemod.Empty:
                        LOGGER.warning('Runner finished but sink queue empty')
                        return None

        train = property(lambda self: self().train)
        apply = property(lambda self: self.Return(self, _pad.Launcher.apply.fget))
        eval = property(lambda self: self.Return(self, _pad.Launcher.eval_traintest.fget))
        tune = property(lambda self: self().tune)

        def __init__(
            self,
            runner: typing.Optional[provcfg.Runner],
            registry: asset.Registry,
            feeds: typing.Optional[typing.Iterable[typing.Union[provcfg.Feed, str, io.Feed]]],
            project: str,
        ):
            self._runner: typing.Optional[provcfg.Runner] = runner
            self._registry: asset.Registry = registry
            self._feeds: typing.Optional[typing.Iterable[typing.Union[provcfg.Feed, str, io.Feed]]] = feeds
            self._project: str = project

        def __call__(self, sink: typing.Optional[io.Sink] = None) -> 'runtime.Launcher':
            return _pad.Platform(self._runner, self._registry, self._feeds, sink).launcher(self._project)

    def __init__(self, package: 'project.Package'):
        self._project: str = package.manifest.name
        self._registry: asset.Registry = volatile.Registry()
        asset.Directory(self._registry).get(self._project).put(package)

    def __call__(
        self,
        runner: typing.Optional[typing.Union[provcfg.Runner, str]] = None,
        feeds: typing.Optional[typing.Iterable[typing.Union[provcfg.Feed, str, io.Feed]]] = None,
    ) -> 'runtime.Virtual.Handler':
        return self.Handler(runner, self._registry, feeds, self._project)

    def __getitem__(self, runner: typing.Union[provcfg.Runner, str]) -> 'runtime.Virtual.Handler':
        """Convenient shortcut for selecting a specific runner using the `launcher[name]` syntax.

        Args:
            runner: Runner alias/qualname to use.

        Returns:
            Launcher handler.
        """
        return self(runner)

    def __getattr__(
        self, mode: str
    ) -> typing.Callable[[typing.Optional[dsl.Native], typing.Optional[dsl.Native]], typing.Any]:
        """Convenient shortcut for accessing the particular launcher mode using the
        ``launcher.train()`` syntax.

        Args:
            mode: Launcher mode to execute.

        Returns:
            Callable launcher handler.
        """
        return getattr(self(), mode)
