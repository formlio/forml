from forml.project import setuptools

setuptools.setup(name='forml-example-titanic',
                 version='0.1.dev0',
                 package_dir={'': 'src'},
                 packages=setuptools.find_packages(where='src'),
                 setup_requires=['pytest-runner', 'pytest-pylint'],
                 tests_require=['pytest-cov', 'pylint', 'pytest'],
                 install_requires=['scikit-learn', 'pandas', 'numpy', 'category_encoders'])
