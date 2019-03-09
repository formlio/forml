import abc
import collections
import typing


class Epoch(collections.namedtuple('Epoch', 'build, id, info')):
    def get(self, state: int)


class Build(collections.namedtuple('Build', 'registry, project, revision, info')):
    def get(self, iteration: typing.Optional[int] = None) -> Epoch:
        ...


class Registry(metaclass=abc.ABCMeta):

    def get(self, project: str, revision: typing.Optional[int] = None) -> Build:
        ...

class Checksum:
    ...

class Persistence:
    def __init__(self, generation: int):
        self._build: Build = ...
        self._generation = collections.defaultdict(lambda: generation)

    def load(self, gid) -> bytes:
        return self._build.get(self._generation[gid]).get(gid)

    def save(self, gid, state) -> Checksum:
        self._generation[gid] += 1

    def commit(self, *checksums: Checksum) -> None: