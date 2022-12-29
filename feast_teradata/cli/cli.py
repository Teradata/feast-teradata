import sys
import click
import re

from importlib.abc import Loader
from importlib.machinery import ModuleSpec
from click.exceptions import BadParameter
from feast.file_utils import replace_str_in_file


def is_valid_name(name: str) -> bool:
    """A name should be alphanumeric values and underscores but not start with an underscore"""
    return not name.startswith("_") and re.compile(r"\W+").search(name) is None


@click.group()
def cli():
    pass


@cli.command()
def init_repo():
    import os
    import importlib.util
    from distutils.dir_util import copy_tree
    from pathlib import Path
    from colorama import Fore, Style

    # check if running in interactive mode or via CI
    is_interactive = os.environ.get("CI_RUNNING", "false").lower() == "false"

    template = "teradata"

    if is_interactive:
        repo_name = click.prompt("Repository Name:")
    else:
        repo_name = os.environ.get("CI_FEAST_REPO_NAME")

    if not is_valid_name(repo_name):
        raise BadParameter(
            message="Name should be alphanumeric values and underscores but not start with an underscore",
            param_hint="PROJECT_DIRECTORY",
        )
    repo_path = Path(os.path.join(Path.cwd(), repo_name))
    repo_path.mkdir(exist_ok=True)
    repo_config_path = repo_path / "feature_store.yaml"

    if repo_config_path.exists():
        new_directory = os.path.relpath(repo_path, os.getcwd())

        print(
            f"The directory {Style.BRIGHT + Fore.GREEN}{new_directory}{Style.RESET_ALL} contains an existing feature "
            f"store repository that may cause a conflict"
        )
        print()
        sys.exit(1)

    # Copy template directory
    template_path = str(Path(Path(__file__).parent / "template" / template).absolute())
    copy_tree(template_path, str(repo_path))

    # Seed the repository
    bootstrap_path = repo_path / "bootstrap.py"
    if not os.path.exists(bootstrap_path):
        raise Exception(f"boostrap.py not found at {bootstrap_path}")

    spec = importlib.util.spec_from_file_location("bootstrap", str(bootstrap_path))
    assert isinstance(spec, ModuleSpec)
    bootstrap = importlib.util.module_from_spec(spec)
    assert isinstance(spec.loader, Loader)
    spec.loader.exec_module(bootstrap)
    bootstrap.bootstrap()  # type: ignore
    os.remove(bootstrap_path)

    # template the feature_store.yaml file
    feature_store_yaml_path = repo_path / "feature_repo" / "feature_store.yaml"
    replace_str_in_file(
        feature_store_yaml_path, "project: my_project", f"project: {repo_name}"
    )

    # Remove the __pycache__ folder if it exists
    import shutil
    shutil.rmtree(repo_path / "__pycache__", ignore_errors=True)

    click.echo()
    click.echo(
        f"Creating a new Feast repository in {Style.BRIGHT + Fore.GREEN}{repo_path}{Style.RESET_ALL}."
    )
    click.echo()


if __name__ == '__main__':
    cli()

