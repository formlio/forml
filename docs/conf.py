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

sys.path.insert(0, os.path.abspath('..'))

import forml  # pylint: disable=wrong-import-position; # noqa: E402

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
    'sphinx_autodoc_typehints',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build']

intersphinx_mapping = {
    'setuptools': ('https://setuptools.pypa.io/en/latest/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
    'openschema': ('https://openschema.readthedocs.io/en/latest/', None),
    'openlake': ('https://openlake.readthedocs.io/en/latest/', None),
}

# Warn about all references where the target cannot be found
nitpicky = False

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
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
    # "google_analytics": ["UA-XXXXX", "auto"],
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
autodoc_typehints = 'description'
autosummary_generate = True

# -- Options for sphinx.ext.napoleon -------------------------------------------
# See: https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html
napoleon_numpy_docstring = False
napoleon_use_rtype = False
napoleon_include_init_with_doc = True

# -- Options for sphinx_autodoc_typehints --------------------------------------
# See: https://pypi.org/project/sphinx-autodoc-typehints/

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
