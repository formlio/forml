"""
Dummy project evaluation.
"""
from forml.flow import task
from forml.project import component
from forml.stdlib.operator import simple

INSTANCE = simple.Consumer(task.Spec('Estimator'))
component.setup(INSTANCE)
