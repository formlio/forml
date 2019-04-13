"""
ForML compiler unit tests.
"""
# pylint: disable=no-self-use

from forml.flow.graph import view
from forml.runtime.assembly import compiler
from forml.runtime.asset import access


def test_generate(path: view.Path, valid_assets: access.Assets):
    """Compiler generate test.
    """
    symbols = compiler.generate(path, valid_assets.state())