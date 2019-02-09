from examples import *
from forml import flow
from forml.flow.operator import ensemble


imputer = SimpleImputer(strategy='mean')
one_hot_enc = OneHotEncoder()
hasher = FeatureHasher(n_features=128)
binarizer = Binarizer(threshold=0.63)
rfc = hasher >> RFC(n_estimators=20, n_jobs=4, max_depth=3)
gbc = GBC(learning_rate=0.3, subsample=0.9, max_depth=5)
bayes = binarizer >> Bayes(alpha=1.1)
lr = LR(max_depth=3)

stack = ensemble.Stack(bases=(gbc, rfc, bayes), folds=4)

composer = flow.Composer(source, imputer >> one_hot_enc >> stack >> lr)

render(composer)
