"""
Main cli frontend.
"""
# pylint: disable=no-self-argument, no-self-use
import typing

from forml import cli, error, conf, etl
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
    def list(cls, project: typing.Optional[str], lineage: typing.Optional[str]) -> None:
        """Repository listing subcommand.

        Args:
            project: Name of project to be listed.
            lineage: Lineage version to be listed.
        """
        level = root.Level(persistent.Registry[conf.REGISTRY.name](**conf.REGISTRY.kwargs))
        if project:
            level = level.get(project)
            if lineage:
                level = level.get(lineage)
        cli.lprint(level.list())

    @cli.Command(help='tune the given project lineage producing new generation', description='Tune mode execution')
    @cli.Param('project', help='project to be tuned')
    @cli.Param('lineage', nargs='?', help='lineage to be tuned')
    @cli.Param('generation', nargs='?', help='generation to be tuned')
    @cli.Param('--lower', help='lower tuneset ordinal')
    @cli.Param('--upper', help='upper tuneset ordinal')
    def tune(cls, project: typing.Optional[str], lineage: typing.Optional[str], generation: typing.Optional[str],
             lower: typing.Optional[etl.OrdinalT], upper: typing.Optional[etl.OrdinalT]) -> None:
        """Tune mode execution.

        Args:
            project: Name of project to be tuned.
            lineage: Lineage version to be tuned.
            generation: Generation index to be tuned.
            lower: Lower ordinal.
            upper: Upper ordinal.
        """
        raise error.Missing(f'Creating project {project}... not implemented')

    @classmethod
    def _runner(cls, project: typing.Optional[str], lineage: typing.Optional[str],
                generation: typing.Optional[str]) -> process.Runner:
        """Common helper for train/apply methods.

        Args:
            project: Project name.
            lineage: Lineage version.
            generation: Generation index.

        Returns: Runner instance.
        """
        registry = root.Level(persistent.Registry[conf.REGISTRY.name](**conf.REGISTRY.kwargs))
        assets = access.Assets(project, lineage, generation, registry)
        engine = etl.Engine[conf.ENGINE.name](**conf.ENGINE.kwargs)
        return process.Runner[conf.RUNNER.name](assets, engine, **conf.RUNNER.kwargs)

    @cli.Command(help='train new generation of given project lineage', description='Train mode execution')
    @cli.Param('project', help='project to be trained')
    @cli.Param('lineage', nargs='?', help='lineage to be trained')
    @cli.Param('generation', nargs='?', help='generation to be trained')
    @cli.Param('--lower', help='lower trainset ordinal')
    @cli.Param('--upper', help='upper trainset ordinal')
    def train(cls, project: typing.Optional[str], lineage: typing.Optional[str], generation: typing.Optional[str],
              lower: typing.Optional[etl.OrdinalT], upper: typing.Optional[etl.OrdinalT]) -> None:
        """Train mode execution.

        Args:
            project: Name of project to be trained.
            lineage: Lineage version to be trained.
            generation: Generation index to be trained.
            lower: Lower ordinal.
            upper: Upper ordinal.
        """
        result = cls._runner(project, lineage, generation).train(lower, upper)
        if result is not None:
            print(result)

    @cli.Command(help='apply given generation of given project lineage', description='Apply mode execution')
    @cli.Param('project', help='project to be applied')
    @cli.Param('lineage', nargs='?', help='lineage to be applied')
    @cli.Param('generation', nargs='?', help='generation to be applied')
    @cli.Param('--lower', help='lower testset ordinal')
    @cli.Param('--upper', help='upper testset ordinal')
    def apply(cls, project: typing.Optional[str], lineage: typing.Optional[str], generation: typing.Optional[str],
              lower: typing.Optional[etl.OrdinalT], upper: typing.Optional[etl.OrdinalT]) -> None:
        """Apply mode execution.

        Args:
            project: Name of project to be applied.
            lineage: Lineage version to be applied.
            generation: Generation index to be applied.
            lower: Lower ordinal.
            upper: Upper ordinal.
        """
        print(cls._runner(project, lineage, generation).apply(lower, upper))
