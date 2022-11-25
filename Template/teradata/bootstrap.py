import click

from feast.file_utils import replace_str_in_file

from teradataml import (
    create_context,
    get_context
)
import getpass

from teradataml import (
    copy_to_sql)
def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`

    import pathlib
    from datetime import datetime, timedelta

    from feast.driver_test_data import create_driver_hourly_stats_df

    repo_path = pathlib.Path(__file__).parent.absolute()
    project_name = str(repo_path)[str(repo_path).rfind("/") + 1 :]
    repo_path = repo_path / "feature_repo"

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)

    driver_entities = [1001, 1002, 1003, 1004, 1005]
    driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)

    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)
    driver_stats_path = data_path / "driver_stats.parquet"
    driver_df.to_parquet(path=str(driver_stats_path), allow_truncated_timestamps=True)

    host = click.prompt("Teradata Host URL: ")
    teradata_user = click.prompt("Teradata User Name:")
    teradata_password = click.prompt("Teradata Password:", hide_input=True)
    teradata_database = click.prompt("Teradata Database Name:")
    teradata_log_mech = click.prompt("Teradata Connection Mechanism:")
    config_file = repo_path / "feature_store.yaml"
    for i in range(2):
        replace_str_in_file(
            config_file, "host", host
        )
        replace_str_in_file(config_file, "Teradata user", teradata_user)
        replace_str_in_file(config_file, "Teradata password", teradata_password)
        replace_str_in_file(config_file, "Teradata database", teradata_database)
        replace_str_in_file(config_file, "Teradata log_mech", teradata_log_mech)

    if click.confirm(
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
            copy_to_sql(df=driver_df,
                        table_name=f"{project_name}_feast_driver_hourly_stats",
                        if_exists="replace")


if __name__ == "__main__":
    bootstrap()