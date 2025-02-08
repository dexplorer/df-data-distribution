import logging
import os

import click
from dist_app.settings import ConfigParms as sc
from dist_app import settings as scg

# Needed to pass the cfg from main app to sub app
from dq_app.settings import ConfigParms as dq_sc
from dq_app import settings as dq_scg
from dqml_app.settings import ConfigParms as dqml_sc
from dqml_app import settings as dqml_scg

from dist_app import dist_app_core as ddc
from utils import logger as ufl


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

    sc.load_config(env)
    # Override sub app config with main app cfg
    dq_scg.APP_ROOT_DIR = scg.APP_ROOT_DIR
    dq_sc.load_config(env)
    dqml_scg.APP_ROOT_DIR = scg.APP_ROOT_DIR
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


def main():
    cli()


if __name__ == "__main__":
    main()
