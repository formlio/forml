"""
Debugging operators.
"""

import os
import secrets
import time

import pandas

from forml.flow import task
from forml.stdlib.operator import simple


@simple.Mapper.operator
class Dump(task.Actor):
    """Pass-through transformer that dumps the input datasets to CSV files.
    """
    SEQ_RESOLUTION = 604800
    CSV_SUFFIX = '.csv'

    def __init__(self, path: str = ''):
        self.dir: str = os.path.dirname(path)
        name, suffix = os.path.splitext(os.path.basename(path))
        self.name: str = name or secrets.token_urlsafe(8)
        self.suffix: str = suffix or self.CSV_SUFFIX

    def apply(self, features: pandas.DataFrame) -> pandas.DataFrame:  # pylint: disable=arguments-differ
        """Transformer logic.

        Args:
            *features: Input frames.

        Returns: Original unchanged frames.
        """
        seq = int(time.time() % self.SEQ_RESOLUTION)
        features.to_csv(os.path.join(self.dir, f'{self.name}-{seq}{self.suffix}'), index=False)
        return features
