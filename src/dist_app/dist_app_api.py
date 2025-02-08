import os
import argparse
import logging

from dist_app.settings import ConfigParms as sc
from dist_app import settings as scg

# Needed to pass the cfg from main app to sub app
from dq_app.settings import ConfigParms as dq_sc
from dq_app import settings as dq_scg
from dqml_app.settings import ConfigParms as dqml_sc
from dqml_app import settings as dqml_scg

from dist_app import dist_app_core as ddc
from utils import logger as ufl

from fastapi import FastAPI
import uvicorn

app = FastAPI()


@app.get("/")
async def root():
    """
    Default route

    Args:
        none

    Returns:
        A default message.
    """

    return {"message": "Data Ingestion App"}


@app.get("/run-distribution-workflow/")
async def run_distribution_workflow(
    distribution_workflow_id: str, cycle_date: str = ""
):
    """
    Runs the distribution workflow.

    Args:
        dataset_id: Id of the dataset.
        cycle_date: Cycle date

    Returns:
        Results from the data reconciliation validations.
    """

    logging.info("Running the distribution workflow %s", distribution_workflow_id)
    ddc.run_distribution_workflow(
        distribution_workflow_id=distribution_workflow_id, cycle_date=cycle_date
    )
    logging.info(
        "Finished running the distribution workflow %s", distribution_workflow_id
    )

    return {"return_code": 0}


def main():
    parser = argparse.ArgumentParser(description="Data Ingestion Application")
    parser.add_argument(
        "-e", "--env", help="Environment", const="dev", nargs="?", default="dev"
    )

    # Get the arguments
    args = vars(parser.parse_args())
    logging.info(args)
    env = args["env"]

    sc.load_config(env)
    # Override sub app config with main app cfg
    dq_scg.APP_ROOT_DIR = scg.APP_ROOT_DIR
    dq_sc.load_config(env)
    dqml_scg.APP_ROOT_DIR = scg.APP_ROOT_DIR
    dqml_sc.load_config(env)

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    ufl.config_logger(log_file_path_name=f"{sc.log_file_path}/{script_name}.log")
    logging.info("Configs are set")

    logging.info("Starting the API service")

    uvicorn.run(
        app,
        port=8080,
        host="0.0.0.0",
        log_config=f"{sc.cfg_file_path}/api_log.ini",
    )

    logging.info("Stopping the API service")


if __name__ == "__main__":
    main()
