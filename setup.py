"""
Setuptools script for ForML package.
"""
import os.path
import sys
import setuptools

sys.path.insert(0, 'src')
import forml

EXTRAS_STDLIB = {
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

EXTRAS_ALL = EXTRAS_STDLIB | EXTRAS_GRAPHVIZ | EXTRAS_DASK | EXTRAS_DOC

setuptools.setup(name='forml',
                 version=forml.__version__,
                 description='Continuous Integration Formalization and Runtime for AI',
                 url='https://github.com/formlio/forml',
                 author='ForML Authors',
                 author_email='noreply@forml.io',
                 license='Apache License 2.0',
                 packages=setuptools.find_packages(where='src'),
                 package_dir={'': 'src'},
                 package_data={'forml.conf': ['*.ini']},
                 setup_requires=['pytest-runner', 'pytest-pylint'],
                 tests_require=['pytest-cov', 'pylint', 'pytest'],
                 install_requires=['joblib', 'pip'],
                 extras_require={
                     'all': EXTRAS_ALL,
                     'doc': EXTRAS_DOC,
                     'stdlib': EXTRAS_STDLIB,
                     'graphviz': EXTRAS_GRAPHVIZ,
                     'dask': EXTRAS_DASK
                 },
                 python_requires='>=3.6',
                 zip_safe=False)
