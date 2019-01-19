"""
Graph topology validation.
"""

# train and apply graph don't intersect
# acyclic
# nodes ports subscriptions:
# * either train or all apply inputs
# * apply outputs match declared szout
# * train/apply subscriptions are exclusive
# * nooutput on trained node
# * no future nodes
