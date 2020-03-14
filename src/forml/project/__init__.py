"""Project setup mechanics.
"""
import pathlib
import typing

from forml.project import product


def open(path: typing.Optional[typing.Union[str, pathlib.Path]] = None,  # pylint: disable=redefined-builtin
         package: typing.Optional[str] = None, **modules: typing.Any) -> product.Artifact:
    """Shortcut for getting a product artifact.

    Args:
        path: Filesystem path to a package root.
        package: Package name.
        **modules: Project module mappings.

    Returns: Product artifact.
    """
    return product.Artifact(path, package, **modules)
