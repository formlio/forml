import os

import pandas


BASE_DIR = os.path.dirname(__file__)
TRAINSET_CSV = os.path.join(BASE_DIR, 'titanic_train.csv')
TESTSET_CSV = os.path.join(BASE_DIR, 'titanic_train.csv')


def trainset(**kwargs) -> pandas.DataFrame:
    return pandas.read_csv(TRAINSET_CSV)


def testset(**kwargs) -> pandas.DataFrame:
    return pandas.read_csv(TESTSET_CSV)
