"""
Setuptools script for ForML package.
"""
import os.path
import setuptools

setuptools.setup(name='forml',
                 version='1.0.dev0',
                 description='Continuous Integration Formalization and Runtime for AI',
                 url='https://github.com/formlio/forml',
                 author='ForML Authors',
                 author_email='noreply@forml.io',
                 license='Apache License 2.0',
                 packages=['forml.' + p for p in setuptools.find_packages(
                     where=os.path.join('src', 'forml'))],
                 package_dir={'': 'src'},
                 setup_requires=['pytest-pylint', 'pytest-runner'],
                 tests_require=['pytest-cov', 'pylint', 'pytest'],
                 install_requires=['joblib', 'pandas'],
                 zip_safe=False)
