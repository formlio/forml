"""
This file exists solely for the sake of the actors defined in conftest.py so that they can use their .get_state()
method that relies on pickling which requires the conftest to be a module, hence this __init__.py.
"""
