"""
Runtime layer.
"""
import logging

import forml

LOGGER = logging.getLogger(__name__)


class Error(forml.Error):
    """Runtime error.
    """
