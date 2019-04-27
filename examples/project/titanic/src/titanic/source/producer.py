"""
Data producer logic.

This module is informal from ForML perspective and has been created just for structuring the project code base.

Note ForML ETL implementation is currently a total stub so this is just a temporal way of feeding data into the system
and the eventual concept will be provided later.
"""

import os

import pandas


BASE_DIR = os.path.dirname(__file__)
TRAINSET_CSV = os.path.join(BASE_DIR, 'titanic_train.csv')
TESTSET_CSV = os.path.join(BASE_DIR, 'titanic_train.csv')


def trainset(**kwargs) -> pandas.DataFrame:
    return pandas.read_csv(TRAINSET_CSV)


def testset(**kwargs) -> pandas.DataFrame:
    return pandas.read_csv(TESTSET_CSV)
