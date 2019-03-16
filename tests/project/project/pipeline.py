"""
Dummy project pipeline.
"""
from forml.flow import task
from forml.project import component
from forml.flow.operator import simple

INSTANCE = simple.Consumer(task.Spec('Estimator'))
component.setup(INSTANCE)
