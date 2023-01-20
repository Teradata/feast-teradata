import click
import os

from feast.file_utils import replace_str_in_file
from teradataml import (
    create_context,
    get_context
)


def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`

    import pathlib
    from datetime import datetime, timedelta

    from feast.driver_test_data import create_driver_hourly_stats_df

    repo_path = pathlib.Path(__file__).parent.absolute()
    project_name = os.path.basename(repo_path)

    repo_path = repo_path / "feature_repo"

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)

    driver_entities = [1001, 1002, 1003, 1004, 1005]
    driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)

    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)
    driver_stats_path = data_path / "driver_stats.parquet"
    driver_df.to_parquet(path=str(driver_stats_path), allow_truncated_timestamps=True)

    # check if running in interactive mode or via CI
    is_interactive = os.environ.get("CI_RUNNING", "false").lower() == "false"
    if is_interactive:
        host = click.prompt("Teradata Host URL")
        teradata_user = click.prompt("Teradata User Name")
        teradata_password = click.prompt("Teradata Password", hide_input=True)
        teradata_database = click.prompt("Teradata Database Name")
        teradata_log_mech = click.prompt("Teradata Connection Mechanism")

    else:
        host = os.environ.get("CI_TD_HOST")
        teradata_user = os.environ.get("CI_TD_USER")
        teradata_password = os.environ.get("CI_TD_PASSWORD")
        teradata_database = os.environ.get("CI_TD_DATABASE")
        teradata_log_mech = os.environ.get("CI_TD_LOGMECH")

    config_file = repo_path / "feature_store.yaml"
    path = 'teradatasql://'+ teradata_user +':' + teradata_password + '@'+host + '/?database=' + teradata_database + '&LOGMECH=' + teradata_log_mech
    replace_str_in_file(config_file, "Teradata registry path", path)
    for i in range(2):
        replace_str_in_file(
            config_file, "Teradata host URL", host
        )
        replace_str_in_file(config_file, "Teradata user", teradata_user)
        replace_str_in_file(config_file, "Teradata password", teradata_password)
        replace_str_in_file(config_file, "Teradata database", teradata_database)
        replace_str_in_file(config_file, "Teradata log_mech", teradata_log_mech)

    if not is_interactive or click.confirm(
        f'Should I upload example data to Teradata (overwriting "{project_name}_feast_driver_hourly_stats" table)?',
        default=True,
    ):
        if get_context() is None:
            create_context(host=host,
                           username=teradata_user,
                           password=teradata_password,
                           database=teradata_database,
                           logmech=teradata_log_mech)
        teradata_conn = get_context().connect()

        with teradata_conn as conn:
            driver_df.to_sql(name=f"{project_name}_feast_driver_hourly_stats", con=conn, if_exists="replace",
                             index=False)
