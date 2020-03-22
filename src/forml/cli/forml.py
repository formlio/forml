"""
Main cli frontend.
"""
# pylint: disable=no-self-argument, no-self-use
import typing

from forml import cli, error, etl
from forml.conf import provider as provcfg
from forml.runtime import process
from forml.runtime.asset import persistent, access
from forml.runtime.asset.directory import root


class Parser(cli.Parser, description='Lifecycle Management for Datascience Projects'):
    """ForML command parser.
    """
    @cli.Command(help='create skeleton for a new project', description='New project setup')
    @cli.Param('name', help='name of a project to be created')
    def init(cls, name: str) -> None:
        """New project setup.

        Args:
            name: Project name to create.
        """
        raise error.Missing(f'Creating project {name}... not implemented')

    @cli.Command(help='show the content of the selected registry', description='Persistent registry listing')
    @cli.Param('project', nargs='?', help='project to be listed')
    @cli.Param('lineage', nargs='?', help='lineage to be listed')
    @cli.Param('-P', '--registry', type=str, help=f'persistent registry reference')
    def list(cls, project: typing.Optional[str], lineage: typing.Optional[str], registry: typing.Optional[str]) -> None:
        """Repository listing subcommand.

        Args:
            project: Name of project to be listed.
            lineage: Lineage version to be listed.
            registry: Optional registry reference.
        """
        regcfg = provcfg.Registry.parse(registry)
        level = root.Level(persistent.Registry[regcfg.name](**regcfg.kwargs))
        if project:
            level = level.get(project)
            if lineage:
                level = level.get(lineage)
        cli.lprint(level.list())

    @classmethod
    def _runner(cls, project: typing.Optional[str], lineage: typing.Optional[str], generation: typing.Optional[str],
                registry: typing.Optional[str], runner: typing.Optional[str],
                engine: typing.Optional[str]) -> process.Runner:
        """Common helper for train/apply methods.

        Args:
            project: Project name.
            lineage: Lineage version.
            generation: Generation index.
            registry: Optional registry reference.
            runner: Optional runner reference.
            engine: Optional engine reference.

        Returns: Runner instance.
        """
        regcfg = provcfg.Registry.parse(registry)
        engcfg = provcfg.Engine.parse(engine)
        runcfg = provcfg.Runner.parse(runner)

        registry = root.Level(persistent.Registry[regcfg.name](**regcfg.kwargs))
        assets = access.Assets(project, lineage, generation, registry)
        engine = etl.Engine[engcfg.name](**engcfg.kwargs)
        return process.Runner[runcfg.name](assets, engine, **runcfg.kwargs)

    @cli.Command(help='tune the given project lineage producing new generation', description='Tune mode execution')
    @cli.Param('project', help='project to be tuned')
    @cli.Param('lineage', nargs='?', help='lineage to be tuned')
    @cli.Param('generation', nargs='?', help='generation to be tuned')
    @cli.Param('-P', '--registry', type=str, help=f'persistent registry reference')
    @cli.Param('-R', '--runner', type=str, help=f'runtime runner reference')
    @cli.Param('-E', '--engine', type=str, help=f'IO engine reference')
    @cli.Param('--lower', help='lower tuneset ordinal')
    @cli.Param('--upper', help='upper tuneset ordinal')
    def tune(cls, project: typing.Optional[str], lineage: typing.Optional[str], generation: typing.Optional[str],
             registry: typing.Optional[str], runner: typing.Optional[str], engine: typing.Optional[str],
             lower: typing.Optional[etl.OrdinalT], upper: typing.Optional[etl.OrdinalT]) -> None:
        """Tune mode execution.

        Args:
            project: Name of project to be tuned.
            lineage: Lineage version to be tuned.
            generation: Generation index to be tuned.
            registry: Optional registry reference.
            runner: Optional runner reference.
            engine: Optional engine reference.
            lower: Lower ordinal.
            upper: Upper ordinal.
        """
        raise error.Missing(f'Creating project {project}... not implemented')

    @cli.Command(help='train new generation of given project lineage', description='Train mode execution')
    @cli.Param('project', help='project to be trained')
    @cli.Param('lineage', nargs='?', help='lineage to be trained')
    @cli.Param('generation', nargs='?', help='generation to be trained')
    @cli.Param('-P', '--registry', type=str, help=f'persistent registry reference')
    @cli.Param('-R', '--runner', type=str, help=f'runtime runner reference')
    @cli.Param('-E', '--engine', type=str, help=f'IO engine reference')
    @cli.Param('--lower', help='lower trainset ordinal')
    @cli.Param('--upper', help='upper trainset ordinal')
    def train(cls, project: typing.Optional[str], lineage: typing.Optional[str], generation: typing.Optional[str],
              registry: typing.Optional[str], runner: typing.Optional[str], engine: typing.Optional[str],
              lower: typing.Optional[etl.OrdinalT], upper: typing.Optional[etl.OrdinalT]) -> None:
        """Train mode execution.

        Args:
            project: Name of project to be trained.
            lineage: Lineage version to be trained.
            generation: Generation index to be trained.
            registry: Optional registry reference.
            runner: Optional runner reference.
            engine: Optional engine reference.
            lower: Lower ordinal.
            upper: Upper ordinal.
        """
        result = cls._runner(project, lineage, generation, registry, runner, engine).train(lower, upper)
        if result is not None:
            print(result)

    @cli.Command(help='apply given generation of given project lineage', description='Apply mode execution')
    @cli.Param('project', help='project to be applied')
    @cli.Param('lineage', nargs='?', help='lineage to be applied')
    @cli.Param('generation', nargs='?', help='generation to be applied')
    @cli.Param('-P', '--registry', type=str, help=f'persistent registry reference')
    @cli.Param('-R', '--runner', type=str, help=f'runtime runner reference')
    @cli.Param('-E', '--engine', type=str, help=f'IO engine reference')
    @cli.Param('--lower', help='lower testset ordinal')
    @cli.Param('--upper', help='upper testset ordinal')
    def apply(cls, project: typing.Optional[str], lineage: typing.Optional[str], generation: typing.Optional[str],
              registry: typing.Optional[str], runner: typing.Optional[str], engine: typing.Optional[str],
              lower: typing.Optional[etl.OrdinalT], upper: typing.Optional[etl.OrdinalT]) -> None:
        """Apply mode execution.

        Args:
            project: Name of project to be applied.
            lineage: Lineage version to be applied.
            generation: Generation index to be applied.
            registry: Optional registry reference.
            runner: Optional runner reference.
            engine: Optional engine reference.
            lower: Lower ordinal.
            upper: Upper ordinal.
        """
        print(cls._runner(project, lineage, generation, registry, runner, engine).apply(lower, upper))
