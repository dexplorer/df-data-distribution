import logging
import os

import click

# from dist_app.settings import ConfigParms as sc
# from dist_app import settings as scg
from config.settings import ConfigParms as sc
from config import settings as scg

# Needed to pass the cfg from main app to sub app
from dq_app.settings import ConfigParms as dq_sc
from dq_app import settings as dq_scg
from dqml_app.settings import ConfigParms as dqml_sc
from dqml_app import settings as dqml_scg

from dist_app import dist_app_core as ddc
from utils import logger as ufl

# Following imports are needed to load test data only.
from ingest_app import ingest_app_core as dic
from ingest_app.settings import ConfigParms as di_sc
from ingest_app import settings as di_scg
from dr_app.settings import ConfigParms as dr_sc
from dr_app import settings as dr_scg
from dp_app.settings import ConfigParms as dp_sc
from dp_app import settings as dp_scg

#
APP_ROOT_DIR = "/workspaces/df-data-distribution"


# Create command group
@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--distribution_workflow_id",
    type=str,
    default="0",
    help="Distribution workflow id",
    required=True,
)
@click.option("--env", type=str, default="dev", help="Environment")
@click.option("--cycle_date", type=str, default="", help="Cycle date")
def run_distribution_workflow(distribution_workflow_id: str, env: str, cycle_date: str):
    """
    Run the distribution workflow.
    """

    scg.APP_ROOT_DIR = APP_ROOT_DIR
    sc.load_config(env)
    # Override sub app config with main app cfg
    dq_scg.APP_ROOT_DIR = APP_ROOT_DIR
    dq_sc.load_config(env)
    dqml_scg.APP_ROOT_DIR = APP_ROOT_DIR
    dqml_sc.load_config(env)

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    ufl.config_logger(log_file_path_name=f"{sc.log_file_path}/{script_name}.log")
    logging.info("Configs are set")

    logging.info("Running the distribution workflow %s", distribution_workflow_id)
    ddc.run_distribution_workflow(
        distribution_workflow_id=distribution_workflow_id, cycle_date=cycle_date
    )
    logging.info(
        "Finished running the distribution workflow %s", distribution_workflow_id
    )


# Following command is needed to load test data only
@cli.command()
@click.option(
    "--ingestion_workflow_id",
    type=str,
    default="0",
    help="Ingestion workflow id",
    required=True,
)
@click.option("--env", type=str, default="dev", help="Environment")
@click.option("--cycle_date", type=str, default="", help="Cycle date")
def run_ingestion_workflow(ingestion_workflow_id: str, env: str, cycle_date: str):
    """
    Run the ingestion workflow.
    """

    scg.APP_ROOT_DIR = APP_ROOT_DIR
    sc.load_config(env)
    # Override sub app config with main app cfg
    di_scg.APP_ROOT_DIR = APP_ROOT_DIR
    di_sc.load_config(env)
    dr_scg.APP_ROOT_DIR = APP_ROOT_DIR
    dr_sc.load_config(env)
    dq_scg.APP_ROOT_DIR = APP_ROOT_DIR
    dq_sc.load_config(env)
    dqml_scg.APP_ROOT_DIR = APP_ROOT_DIR
    dqml_sc.load_config(env)
    dp_scg.APP_ROOT_DIR = APP_ROOT_DIR
    dp_sc.load_config(env)
    dp_scg.APP_ROOT_DIR = APP_ROOT_DIR
    dp_sc.load_config(env)

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    ufl.config_logger(log_file_path_name=f"{sc.log_file_path}/{script_name}.log")
    logging.info("Configs are set")

    logging.info("Running the ingestion workflow %s", ingestion_workflow_id)
    dic.run_ingestion_workflow(
        ingestion_workflow_id=ingestion_workflow_id, cycle_date=cycle_date
    )
    logging.info("Finished running the ingestion workflow %s", ingestion_workflow_id)


def main():
    cli()


if __name__ == "__main__":
    main()
