import logging
import os

import click
from dotenv import load_dotenv
from config.settings import ConfigParms as sc
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
    help="Distribution workflow id",
    required=True,
)
@click.option("--cycle_date", type=str, default="", help="Cycle date")
def run_distribution_workflow(distribution_workflow_id: str, cycle_date: str):
    """
    Run the distribution workflow.
    """

    logging.info("Running the distribution workflow %s", distribution_workflow_id)
    ddc.run_distribution_workflow(
        distribution_workflow_id=distribution_workflow_id, cycle_date=cycle_date
    )
    logging.info(
        "Finished running the distribution workflow %s", distribution_workflow_id
    )


def main():
    # Load the environment variables from .env file
    load_dotenv()

    # Fail if env variable is not set
    sc.load_config()

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    ufl.config_logger(log_file_path_name=f"{sc.app_log_dir}/{script_name}.log")
    logging.info("Configs are set")
    logging.info(os.environ)
    logging.info(sc.config)
    logging.info(vars(sc))

    cli()


if __name__ == "__main__":
    main()
