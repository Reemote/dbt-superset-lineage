import typer
from .superset_api import Superset
from .push_physical_datasets import main as physicals
from .push_virtual_datasets import main as virtuals

app = typer.Typer()


@app.command()
def push_virtual_datasets(datasets_dir: str = typer.Option('.', help="Directory with dataset definitions."),
                      superset_url: str = typer.Argument(..., help="URL of your Superset, e.g. "
                                                                   "https://mysuperset.mycompany.com"),
                      superset_db_id: int = typer.Option(None, help="ID of your database within Superset towards which "
                                                                    "the push should be reduced to run."),
                      superset_refresh_columns: bool = typer.Option(False, help="Whether columns in Superset should be "
                                                                                "refreshed from database before "
                                                                                "the push."),
                      superset_access_token: str = typer.Option(None, envvar="SUPERSET_ACCESS_TOKEN",
                                                                help="Access token to Superset API."
                                                                     "Can be automatically generated if "
                                                                     "SUPERSET_REFRESH_TOKEN is provided."),
                      superset_refresh_token: str = typer.Option(None, envvar="SUPERSET_REFRESH_TOKEN",
                                                                 help="Refresh token to Superset API."),
                      superset_user: str = typer.Option(None, envvar="SUPERSET_USER",
                                                                help="Superset Username"),
                      superset_password: str = typer.Option(None, envvar="SUPERSET_PASSWORD",
                                                                 help="Password of the Superset user.")):
     # require at least one token for Superset or a username/password combination
     assert superset_access_token is not None or superset_refresh_token is not None or (superset_user is not None and superset_password is not None), \
           "Add `SUPERSET_ACCESS_TOKEN or SUPERSET_REFRESH_TOKEN " \
           "or  (SUPERSET_USER and SUPERSET_PASSWORD) " \
           "to your environment variables or provide in CLI " \
           "via --superset-access-token or --superset-refresh-token " \
           "or (--superset-user and --superset-password)."

     superset = Superset(superset_url + '/api/v1',
                        access_token = superset_access_token,
                        refresh_token = superset_refresh_token,
                        user = superset_user,
                        password = superset_password)

     virtuals(datasets_dir, superset_db_id, superset_refresh_columns, superset)


@app.command()
def push_physical_datasets(dbt_project_dir: str = typer.Option('.', help="Directory path to dbt project."),
                      dbt_db_name: str = typer.Option(None, help="Name of your database within dbt to which the script "
                                                                 "should be reduced to run."),
                      superset_url: str = typer.Argument(..., help="URL of your Superset, e.g. "
                                                                   "https://mysuperset.mycompany.com"),
                      superset_db_id: int = typer.Option(None, help="ID of your database within Superset towards which "
                                                                    "the push should be reduced to run."),
                      superset_debug_dir: str = typer.Option(None, envvar="SUPERSET_DEBUG_DIR",
                                                             help="A path to a directory where debugging files  "
                                                                  "will be placed if this option is specified."),
                      superset_refresh_columns: bool = typer.Option(False, help="Whether columns in Superset should be "
                                                                                "refreshed from database before "
                                                                                "the push."),
                      superset_access_token: str = typer.Option(None, envvar="SUPERSET_ACCESS_TOKEN",
                                                                help="Access token to Superset API."
                                                                     "Can be automatically generated if "
                                                                     "SUPERSET_REFRESH_TOKEN is provided."),
                      superset_refresh_token: str = typer.Option(None, envvar="SUPERSET_REFRESH_TOKEN",
                                                                 help="Refresh token to Superset API."),
                      superset_user: str = typer.Option(None, envvar="SUPERSET_USER",
                                                                help="Superset Username"),
                      superset_password: str = typer.Option(None, envvar="SUPERSET_PASSWORD",
                                                                 help="Password of the Superset user.")):
     # require at least one token for Superset or a username/password combination
     assert superset_access_token is not None or superset_refresh_token is not None or (superset_user is not None and superset_password is not None), \
           "Add `SUPERSET_ACCESS_TOKEN or SUPERSET_REFRESH_TOKEN " \
           "or  (SUPERSET_USER and SUPERSET_PASSWORD) " \
           "to your environment variables or provide in CLI " \
           "via --superset-access-token or --superset-refresh-token " \
           "or (--superset-user and --superset-password)."

     superset = Superset(superset_url + '/api/v1',
                        access_token = superset_access_token,
                        refresh_token = superset_refresh_token,
                        user = superset_user,
                        password = superset_password)

     physicals(dbt_project_dir, dbt_db_name, superset_db_id, superset_debug_dir, superset_refresh_columns, superset)


if __name__ == '__main__':
    app()
