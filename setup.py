#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# 'License'); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Setuptools script for ForML package.
"""
import os

import setuptools

EXTRAS_DASK = {'dask'}

EXTRAS_DEV = {
    'black',
    'flake8-colors',
    'flake8-bugbear',
    'flake8-typing-imports',
    'isort',
    'pip-tools',
    'pre-commit',
    'pycln',
    'pylint',
    'pytest-cov',
    'pytest-asyncio',
}

EXTRAS_DOCS = {
    'sphinx',
    'sphinx-copybutton',
    'sphinx-immaterial',
    'sphinxcontrib-details-directive',
    'sphinxcontrib-napoleon',
    'nbsphinx',  # also needs pandoc binary
    'openlake',
}

EXTRAS_GRAPHVIZ = {'graphviz'}

EXTRAS_MLFLOW = {'mlflow'}

EXTRAS_REST = {'starlette', 'uvicorn'}

EXTRAS_SQL = {'pyhive', 'sqlalchemy'}

EXTRAS_ALL = EXTRAS_DASK | EXTRAS_DEV | EXTRAS_DOCS | EXTRAS_MLFLOW | EXTRAS_GRAPHVIZ | EXTRAS_REST | EXTRAS_SQL

setuptools.setup(
    name='forml',
    description='Lifecycle management framework for Data science projects',
    long_description=open('README.md', encoding='utf8').read(),  # pylint: disable=consider-using-with
    long_description_content_type='text/markdown',
    url='https://github.com/formlio/forml',
    maintainer='ForML Development Team',
    maintainer_email='forml-dev@googlegroups.com',
    license='Apache License 2.0',
    packages=setuptools.find_packages(include=['forml*'], where=os.path.dirname(__file__)),
    package_data={'forml.conf': ['config.toml', 'logging.ini']},
    setup_requires=['setuptools', 'wheel', 'tomli'],
    install_requires=[
        'click',
        'cloudpickle',
        'numpy',
        'packaging>=20.0',
        'pandas',
        'pip',
        'scikit-learn',
        'setuptools',
        'tomli',
    ],
    extras_require={
        'all': EXTRAS_ALL,
        'dask': EXTRAS_DASK,
        'dev': EXTRAS_DEV,
        'docs': EXTRAS_DOCS,
        'graphviz': EXTRAS_GRAPHVIZ,
        'mlflow': EXTRAS_MLFLOW,
        'rest': EXTRAS_REST,
        'sql': EXTRAS_SQL,
    },
    entry_points={
        'console_scripts': [
            'forml = forml.setup:cli',
        ]
    },
    python_requires='>=3.9',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'Topic :: System :: Distributed Computing',
    ],
    zip_safe=False,
)
