# train and apply graph don't intersect
# each node gets state and in train flow sends state
# acyclic
# nodes ports subscriptions:
# * either train or all apply inputs
# * if train input then also state input and output
# * apply outputs match declared szout
# * train/apply subscriptions are exclusive
