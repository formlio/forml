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
import sys

import setuptools

sys.path.insert(0, 'src')
import forml  # noqa: E402

EXTRAS_FLOW = {
    'pandas',
    'scikit-learn'
}

EXTRAS_GRAPHVIZ = {
    'graphviz'
}

EXTRAS_DASK = {
    'dask',
    'cloudpickle'
}

EXTRAS_DOC = {
    'sphinx'
}

EXTRAS_DEV = {
    'pre-commit',
    'flake8',
    'mypy'
}

EXTRAS_ALL = EXTRAS_FLOW | EXTRAS_GRAPHVIZ | EXTRAS_DASK | EXTRAS_DEV | EXTRAS_DOC

setuptools.setup(name='forml',
                 version=forml.__version__,
                 description='Lifecycle management framework for Data science projects',
                 long_description=open('README.md', 'r').read(),
                 long_description_content_type='text/markdown',
                 url='https://github.com/formlio/forml',
                 maintainer='ForML Development Team',
                 maintainer_email='noreply@forml.io',
                 license='Apache License 2.0',
                 packages=setuptools.find_packages(where='src'),
                 package_dir={'': 'src'},
                 package_data={'forml.conf': ['config.toml', 'logging.ini']},
                 setup_requires=['pytest-runner', 'pytest-pylint', 'pytest-flake8'],
                 tests_require=['pytest-cov', 'pylint', 'pytest', 'cloudpickle'],
                 install_requires=['joblib', 'pip', 'setuptools', 'packaging>=20.0', 'toml'],
                 extras_require={
                     'all': EXTRAS_ALL,
                     'dask': EXTRAS_DASK,
                     'dev': EXTRAS_DEV,
                     'doc': EXTRAS_DOC,
                     'flow': EXTRAS_FLOW,
                     'graphviz': EXTRAS_GRAPHVIZ,
                 },
                 entry_points={'console_scripts': [
                     'forml = forml.cli.forml:Parser',
                 ]},
                 python_requires='>=3.6',
                 classifiers=[
                     'Development Status :: 2 - Pre-Alpha',
                     'Environment :: Console',
                     'Intended Audience :: Developers',
                     'Intended Audience :: Science/Research',
                     'License :: OSI Approved :: Apache Software License',
                     'Programming Language :: Python :: 3',
                     'Topic :: Scientific/Engineering :: Artificial Intelligence',
                     'Topic :: System :: Distributed Computing'
                 ],
                 zip_safe=False)
