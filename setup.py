"""
Setuptools script for ForML package.
"""
import os.path
import setuptools

setuptools.setup(name='forml',
                 version='0.1.dev0',
                 description='Continuous Integration Formalization and Runtime for AI',
                 url='https://github.com/formlio/forml',
                 author='ForML Authors',
                 author_email='noreply@forml.io',
                 license='Apache License 2.0',
                 packages=setuptools.find_packages(where='src'),
                 package_dir={'': 'src'},
                 setup_requires=['pytest-runner', 'pytest-pylint'],
                 tests_require=['pytest-cov', 'pylint', 'pytest'],
                 install_requires=['joblib', 'pandas', 'scikit-learn', 'cloudpickle'],
                 extras_require={
                     'graphviz': ['graphviz'],
                     'dask': ['dask']
                 },
                 zip_safe=False)
