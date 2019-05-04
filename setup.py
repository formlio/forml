"""
Setuptools script for ForML package.
"""
import os.path
import setuptools

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

EXTRAS_ALL = EXTRAS_DASK | EXTRAS_GRAPHVIZ | EXTRAS_DASK

setuptools.setup(name='forml',
                 version='0.1.dev0',
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
                 install_requires=['joblib'],
                 extras_require={
                     'all': EXTRAS_ALL,
                     'stdlib': EXTRAS_STDLIB,
                     'graphviz': EXTRAS_GRAPHVIZ,
                     'dask': EXTRAS_DASK
                 },
                 python_requires='>=3.6',
                 zip_safe=False)
