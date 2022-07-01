# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Configuration file for the Sphinx documentation builder.

This file only contains a selection of the most common options. For a full
list see the documentation:
http://www.sphinx-doc.org/en/master/config
"""
# pylint: disable=invalid-name

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
import typing

from sphinx import application
from sphinx.ext import autodoc, autosummary

sys.path.insert(0, os.path.abspath('..'))

import forml  # pylint: disable=wrong-import-position; # noqa: E402
from forml import provider  # pylint: disable=wrong-import-position; # noqa: E402

# -- Project information -----------------------------------------------------

project = 'ForML'

# The full version, including alpha/beta/rc tags
release = forml.__version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.coverage',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
    'sphinx_immaterial',
    'sphinx_copybutton',
    'sphinxcontrib.details.directive',
    'nbsphinx',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build']

intersphinx_mapping = {
    'dask': ('https://docs.dask.org/en/stable/', None),
    'graphviz': ('https://graphviz.readthedocs.io/en/stable/', None),
    'jupyter': ('https://docs.jupyter.org/en/latest/', None),
    'mlflow': ('https://mlflow.org/docs/latest/', None),
    'openlake': ('https://openlake.readthedocs.io/en/latest/', None),
    'openschema': ('https://openschema.readthedocs.io/en/latest/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
    'pip': ('https://pip.pypa.io/en/stable/', None),
    'python': ('https://docs.python.org/3', None),
    'setuptools': ('https://setuptools.pypa.io/en/latest/', None),
    'sklearn': ('https://scikit-learn.org/stable/', None),
    'sqlalchemy': ('https://docs.sqlalchemy.org/en/latest/', None),
}

# Warn about all references where the target cannot be found
nitpicky = True
_target_blacklist = {
    'py:class': (
        'pandas.core.generic.NDFrame',
        '_Actor',
        r'.+\[.*dsl.Ordering.Direction.*\].*',
    ),
    'py:.*': (r'(?:forml|asset|dsl|flow|project)\..*',),
}
nitpick_ignore_regex = [(k, v) for k, t in _target_blacklist.items() for v in t]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'sphinx_immaterial'

# Set link name generated in the top bar.
html_title = 'ForML'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

html_show_sourcelink = True
html_show_copyright = False
html_show_sphinx = False
html_favicon = '_static/images/favicon.ico'
html_logo = '_static/images/logo.svg'

html_theme_options = {
    'icon': {
        'repo': 'fontawesome/brands/github',
    },
    'site_url': 'https://forml.io/',
    'repo_url': 'https://github.com/formlio/forml/',
    'repo_name': 'formlio/forml',
    'repo_type': 'github',
    'edit_uri': 'blob/main/docs',
    'google_analytics': ['G-GGNVJ2N8MF', 'auto'],
    'features': [
        # "navigation.expand",
        # "navigation.tabs",
        # "toc.integrate",
        'navigation.sections',
        'navigation.instant',
        # "header.autohide",
        'navigation.top',
        'navigation.tracking',
        'search.highlight',
        'search.share',
    ],
    'toc_title_is_page_title': True,
    # 'globaltoc_collapse': False,
    'palette': [
        {
            'media': '(prefers-color-scheme: light)',
            'scheme': 'default',
            'primary': 'blue',
            'accent': 'cyan',
            'toggle': {
                'icon': 'material/weather-night',
                'name': 'Switch to dark mode',
            },
        },
        {
            'media': '(prefers-color-scheme: dark)',
            'scheme': 'slate',
            'primary': 'blue',
            'accent': 'cyan',
            'toggle': {
                'icon': 'material/weather-sunny',
                'name': 'Switch to light mode',
            },
        },
    ],
}

# == Extensions configuration ==================================================

# -- Options for sphinx.ext.autodoc --------------------------------------------
# See: https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html
autoclass_content = 'both'
autodoc_typehints = 'signature'
autosummary_generate = True
autodoc_member_order = 'bysource'


# -- Options for sphinx.ext.napoleon -------------------------------------------
# See: https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html
napoleon_numpy_docstring = False


# -- Options for sphinx_immaterial --------------------------------------
# See: https://pypi.org/project/sphinx-immaterial/
# python_transform_type_annotations_concise_literal = False
# python_transform_type_annotations_pep604 = False
object_description_options = [
    ('py:.*parameter', dict(include_in_toc=False)),
    # ('py:.*function', dict(include_in_toc=False)),
    # ('py:.*property', dict(include_in_toc=False)),
    # ('py:.*method', dict(include_in_toc=False)),
    ('py:.*', dict(include_fields_in_toc=False)),
]


# -- Options for nbsphinx --------------------------------------
# See: https://nbsphinx.readthedocs.io/en/latest/
nbsphinx_requirejs_path = ''


class ProviderDocumenter(autodoc.ClassDocumenter):
    """Custom documenter for ForML provider implementations to workaround autodoc's signature
    detection problems.
    """

    @classmethod
    def can_document_member(cls, member: typing.Any, membername: str, isattr: bool, parent: typing.Any) -> bool:
        return super().can_document_member(member, membername, isattr, parent) and issubclass(member, provider.Service)

    def get_attr(self, obj: typing.Any, name: str, *defargs: typing.Any) -> typing.Any:
        """Autodoc is detecting ForML providers to have __call__ (due to their metaclass) and takes
        the signature from there instead of __init__.
        """
        if name == '__call__':
            return None
        return super().get_attr(obj, name, *defargs)


class Autosummary(autosummary.Autosummary):
    """Patched Autosummary with custom formatting for ForML providers."""

    @staticmethod
    def __format_name(display_name, sig, summary, real_name):
        """Custom name formatting."""
        if display_name.startswith(provider.__name__):
            display_name = display_name.rsplit('.', 2)[-2].title()
        return display_name, sig, summary, real_name

    def get_items(self, names):
        return [self.__format_name(*i) for i in super().get_items(names)]


def setup(app: application.Sphinx):
    """Sphinx setup hook."""
    app.add_autodocumenter(ProviderDocumenter)
    app.add_directive('autosummary', Autosummary)
