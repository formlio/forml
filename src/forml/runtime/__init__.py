"""
Runtime layer.
"""
import logging

import forml
from forml.runtime import resource, persistent

LOGGER = logging.getLogger(__name__)


class Error(forml.Error):
    """Runtime error.
    """
