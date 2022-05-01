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
ForML mlflow registry unit tests.
"""
# pylint: disable=no-self-use
import os
import random
import signal
import string
import subprocess
import time
import typing

import pytest
import requests
from requests import exceptions

from forml.extension.registry import mlflow
from forml.io import asset

from . import Registry


class TestRegistry(Registry):
    """Registry unit tests."""

    MLFLOW_HOST = '127.0.0.1'
    MLFLOW_PORT = 55555
    GUNICFG_MODULE = 'gunicfg'
    GUNISRV_HEADER = f'formltest-{"".join(random.choice(string.ascii_lowercase) for _ in range(8))}'

    @classmethod
    @pytest.fixture(scope='session')
    def constructor(
        cls, tmp_path_factory: pytest.TempPathFactory
    ) -> typing.Iterable[typing.Callable[[], asset.Registry]]:
        tmpdir = tmp_path_factory.mktemp('mlflow-registry')
        with open(tmpdir / f'{cls.GUNICFG_MODULE}.py', 'w', encoding='utf-8') as cfg:
            cfg.write(f'import gunicorn\ngunicorn.SERVER = "{cls.GUNISRV_HEADER}"\n')
        os.chdir(tmpdir)  # so that gunicfg is on import path
        with subprocess.Popen(  # pylint: disable=subprocess-popen-preexec-fn
            [
                'mlflow',
                'server',
                '--host',
                cls.MLFLOW_HOST,
                '--port',
                str(cls.MLFLOW_PORT),
                '--backend-store-uri',
                f'sqlite:///{tmpdir}/mlruns.sqlite',
                '--default-artifact-root',
                f'file:{tmpdir}/artifacts',
                '--workers',
                '1',
                '--gunicorn-opts',
                f'--config python:{cls.GUNICFG_MODULE}',
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid,
        ) as server:
            uri = f'http://{cls.MLFLOW_HOST}:{cls.MLFLOW_PORT}'
            while server.poll() is None:  # wait for successful startup
                try:
                    if requests.head(uri).headers.get('server') == cls.GUNISRV_HEADER:
                        break
                except exceptions.RequestException:
                    pass
                time.sleep(1)
            else:
                raise RuntimeError(f'Unable to start MLFlow server: {server.communicate()}')

            yield lambda: mlflow.Registry(uri, repoid=f'test{random.randrange(100000)}')

            os.killpg(os.getpgid(server.pid), signal.SIGTERM)
