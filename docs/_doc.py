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
Sphinx customization.
"""

import re
import typing

from enchant import tokenize
from sphinx.ext import autodoc, autosummary

import forml
from forml import provider
from forml.io import dsl
from forml.io.dsl import function

VERSION = forml.__version__


class ClassDocumenter(autodoc.ClassDocumenter):
    """Custom class documenter with ForML specific workarounds."""

    def import_object(self, raiseerror: bool = False) -> bool:
        """The dsl.Schema is not rendered properly due to the fact it is an object rather than a
        class.
        """
        ret = super().import_object(raiseerror)
        if isinstance(self.object, dsl.Schema.__class__):
            self.object = self.object.__class__
            self.doc_as_attr = False
        return ret

    def get_attr(self, obj: typing.Any, name: str, *defargs: typing.Any) -> typing.Any:
        """Autodoc is detecting ForML providers to have __call__ (due to their metaclass) and takes
        the signature from there instead of __init__.
        """
        if obj is provider.Meta and name == '__call__':
            return None
        return super().get_attr(obj, name, *defargs)


class MethodDocumenter(autodoc.MethodDocumenter):
    """Custom method documenter with ForML specific workarounds."""

    def import_object(self, raiseerror: bool = False) -> bool:
        """The dsl.Schema methods are not rendered properly due to the fact it is an object rather
        than a class.
        """
        ret = super().import_object(raiseerror)
        if isinstance(self.parent, dsl.Schema.__class__):
            self.parent = self.parent.__class__
        return ret


class Autosummary(autosummary.Autosummary):
    """Patched Autosummary with custom formatting for certain ForML types."""

    RE_PROVIDER = re.compile(rf'(?={provider.__name__})(?:\w+\.)+(\w+)\.\w+$')
    RE_DSL = re.compile(rf'(?={dsl.__name__})(?:\w+\.)+(\w+\.\w+)$')
    RE_FUNCTION = re.compile(rf'(?={function.__name__})(?:\w+\.)+_(\w+)$')

    @classmethod
    def __format_name(cls, display_name: str, sig: str, summary: str, real_name: str):
        """Custom name formatting."""
        if match := cls.RE_PROVIDER.match(display_name):
            display_name = match.group(1).title()
        elif match := cls.RE_FUNCTION.match(display_name):
            display_name = match.group(1).title()
        elif match := cls.RE_DSL.match(display_name):
            display_name = match.group(1)
        return display_name, sig, summary, real_name

    def get_items(self, names):
        return [self.__format_name(*i) for i in super().get_items(names)]


class Filter(tokenize.Filter):
    """Custom spell-checking filter."""

    SUFFIXES = 'py', 'toml'
    """File suffixes."""

    def _skip(self, word):
        return any(word.endswith(f'.{s}') for s in self.SUFFIXES)
