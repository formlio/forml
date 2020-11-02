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
ETL layer.
"""
import collections
import typing

from forml import error
from forml.flow.pipeline import topology
from forml.io.dsl.struct import series, frame
from forml.project import product


class Source(typing.NamedTuple):  # TODO: move to component
    """Feed independent data provider description.
    """
    extract: 'Source.Extract'
    transform: typing.Optional[topology.Composable] = None

    class Extract(collections.namedtuple('Extract', 'train, apply, label, ordinal')):
        """Combo of select statements for the different modes.
        """
        train: frame.Query
        apply: frame.Query
        label: typing.Tuple[series.Column]
        ordinal: typing.Optional[series.Operable]

        def __new__(cls, train: frame.Queryable, apply: frame.Queryable, label: typing.Sequence[series.Column],
                    ordinal: typing.Optional[series.Operable]):
            train = train.query
            apply = apply.query
            if {c.operable for c in train.columns}.intersection(c.operable for c in label):
                raise error.Invalid('Label-feature overlap')
            if train.schema != apply.schema:
                raise error.Invalid('Train-apply schema mismatch')
            if ordinal:
                ordinal = series.Operable.ensure_is(ordinal)
            return super().__new__(cls, train, apply, tuple(label), ordinal)

    @classmethod
    def query(cls, features: frame.Queryable, *label: series.Column, apply: typing.Optional[frame.Queryable] = None,
              ordinal: typing.Optional[series.Operable] = None) -> 'Source':
        """Create new source with the given extraction.

        Args:
            features: Query defining the train (and possibly apply) features.
            label: List of training label columns.
            apply: Optional query defining the apply features (if different from train ones).
            ordinal: Optional specification of an ordinal column.

        Returns: New source instance.
        """
        return cls(cls.Extract(features, apply or features, label, ordinal))  # pylint: disable=no-member

    def __rshift__(self, transform: topology.Composable) -> 'Source':
        return self.__class__(self.extract, self.transform >> transform if self.transform else transform)

    def bind(self, pipeline: typing.Union[str, topology.Composable], **modules: typing.Any) -> 'product.Artifact':
        """Create an artifact from this source and given pipeline.

        Args:
            pipeline: Pipeline to create the artifact with.
            **modules: Other optional artifact modules.

        Returns: Project artifact instance.
        """
        return product.Artifact(source=self, pipeline=pipeline, **modules)
